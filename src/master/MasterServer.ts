import { StorageType } from './../common/types';
import { WorkerClient } from '../worker/WorkerClient';
import { Request, Response } from '../client/Client';
import { SerializedFunction, serialize } from '../common/SerializeFunction';

import * as workerActions from '../worker/handlers';
import * as masterActions from './handlers';
import debug, { setDebugFunc } from '../common/debug';
import { processRequest } from '../common/handler';

type InArgs = {
  type: 'value' | 'partitions' | 'parts';
  args?: any[];
  partitions?: string[];
  parts?: string[][];
};

type OutArgs = {
  type: 'reduce' | 'partitions' | 'parts' | 'saveFile';
  storageType?: StorageType;
  saveFunc?: SerializedFunction<
    (data: any[], filename: string) => void | Promise<void>
  >;
  partitionFunc?: SerializedFunction<
    (v: any[], arg: any, partitionIndex: number) => any[][]
  >;
  args?: any[];
};

type Partition = [number, string];

type CacheRecord = {
  storageType: StorageType;
  partitions: Partition[];
};

export interface FileLoader {
  canHandleUrl(
    baseUrl: string,
    type: 'load' | 'save',
  ): boolean | Promise<boolean>;
  listFiles(baseUrl: string, recursive?: boolean): string[] | Promise<string[]>;
  createDataLoader(
    baseUrl: string,
  ): SerializedFunction<(filename: string) => Buffer | Promise<Buffer>>;

  initSaveProgress(baseUrl: string, overwrite?: boolean): void | Promise<void>;
  createDataSaver(
    baseUrl: string,
  ): SerializedFunction<
    (filename: string, buffer: Buffer) => void | Promise<void>
  >;
  markSaveSuccess(baseUrl: string): void | Promise<void>;
}

interface TaskDetail {
  index: number;
  total: number;
}

export class MasterServer {
  workers: WorkerClient[] = [];
  caches: CacheRecord[] = [];
  cacheIdCounter: number = 0;

  fileLoaderRegistry: FileLoader[] = [];

  constructor() {
    setDebugFunc(require('debug')('dcf:master'));
  }

  registerFileLoader(loader: FileLoader) {
    this.fileLoaderRegistry.push(loader);
  }

  async init(): Promise<void> {}
  async dispose(): Promise<void> {
    for (const id of Object.keys(this.caches)) {
      await this.releaseCache(id as any);
    }
  }

  send(m: Response) {
    throw new Error('Must be implemented.');
  }

  onTaskBegin(task: TaskDetail, progressTotal: number) {
    this.send({
      type: 'task',
      partitions: progressTotal,
      taskIndex: ++task.index,
      tasks: task.total,
    });
  }

  async processRequest<T>(m: Request<T>): Promise<any> {
    return processRequest(m, this);
  }

  // TODO: make split more even.
  // change [3, 3, 3, 2, 2] into [3, 2, 3, 2, 3]
  getSplitIndecies(count: number): number[][] {
    const ret: number[][] = [];
    const workerCount = this.workers.length;
    for (let i = 0; i < workerCount; i++) {
      ret.push([]);
    }
    for (let i = 0; i < count; i++) {
      ret[i % workerCount].push(i);
    }
    return ret;
  }

  splitByWorker<T>(args: T[]): T[][] {
    const ret: T[][] = [];
    for (const worker of this.workers) {
      ret.push([]);
    }
    const count = this.workers.length;
    for (const [i, item] of args.entries()) {
      ret[i % count].push(item);
    }
    return ret;
  }

  mergeWorkerResult<T>(resps: T[][]): T[] {
    const ret = [];
    for (let index = 0; ; index++) {
      for (const resp of resps) {
        if (index >= resp.length) {
          return ret;
        }
        ret.push(resp[index]);
      }
    }
  }

  // final work for even distribution.
  async finalWork(
    ins: InArgs[],
    indecies: number[][],
    out: OutArgs | OutArgs[],
    mappers: SerializedFunction<(arg: any, partitionIndex: number) => any>[],
    task: TaskDetail,
    progressTotal: number,
  ): Promise<any[]> {
    this.onTaskBegin(task, progressTotal);
    const works: Promise<any>[] = [];
    for (const [i, worker] of this.workers.entries()) {
      works.push(
        worker.processRequest({
          type: workerActions.CALC,
          payload: {
            in: ins[i],
            indecies,
            mappers,
            out: Array.isArray(out) ? out[i] : out,
          },
        }),
      );
    }
    const resp = await Promise.all(works);

    for (let i = 0; i < this.workers.length; i++) {
      const workerOut = Array.isArray(out) ? out[i] : out;
      if (workerOut.type === 'partitions') {
        resp[i] = resp[i].map((v: string) => [i, v] as Partition);
      }
      if (workerOut.type === 'parts') {
        return resp;
      }
    }
    return this.mergeWorkerResult(resp);
  }

  createRDD(
    args: any[],
    out: OutArgs | OutArgs[],
    mappers: SerializedFunction<(arg: any, partitionIndex: number) => any>[],
    task: TaskDetail,
  ): Promise<any> {
    const indecies = this.getSplitIndecies(args.length);
    const partitionArgs = this.splitByWorker(args);

    if (!Array.isArray(out) && Array.isArray(out.args)) {
      out = this.splitByWorker(out.args).map(
        args =>
          ({
            ...out,
            args,
          } as OutArgs),
      );
    }

    return this.finalWork(
      partitionArgs.map(
        v =>
          ({
            type: 'value',
            args: v,
          } as InArgs),
      ),
      indecies,
      out,
      mappers,
      task,
      args.length,
    );
  }

  addCache(storageType: StorageType, partitions: Partition[]) {
    const id = ++this.cacheIdCounter;
    this.caches[id] = {
      storageType,
      partitions,
    };
    return id;
  }

  async loadCache(
    id: number,
    out: OutArgs | OutArgs[],
    mappers: SerializedFunction<(arg: any, partitionIndex: number) => any>[],
    task: TaskDetail,
  ): Promise<any> {
    const { storageType, partitions } = this.caches[id];

    this.onTaskBegin(task, partitions.length);

    // group partitions by workers.
    let inArgs = this.workers.map(v => ({
      in: {
        type: 'partitions',
        storageType,
        partitions: [] as string[],
      },
      indecies: [] as number[],
    }));

    for (let [i, [wid, pid]] of partitions.entries()) {
      inArgs[wid].in.partitions.push(pid);
      inArgs[wid].indecies.push(i);
    }

    if (!Array.isArray(out) && Array.isArray(out.args)) {
      const out1: OutArgs[] = this.workers.map(
        v => ({ ...out, args: [] } as OutArgs),
      );
      for (let [i, [wid, pid]] of partitions.entries()) {
        (out1[wid].args as any[]).push(out.args[i]);
      }
      out = out1;
    }

    const works: Promise<any>[] = [];
    for (const [i, worker] of this.workers.entries()) {
      works.push(
        worker.processRequest({
          type: workerActions.CALC,
          payload: {
            in: inArgs[i].in,
            indecies: inArgs[i].indecies,
            mappers,
            out: Array.isArray(out) ? out[i] : out,
          },
        }),
      );
    }
    const resps = await Promise.all(works);

    const ret = partitions.map(v => null);

    const firstOut = Array.isArray(out) ? out[0] : out;
    if (firstOut.type === 'parts') {
      return resps;
    }

    // flat map result by indecies
    for (let [i, resp] of resps.entries()) {
      const workerOut = Array.isArray(out) ? out[i] : out;
      if (workerOut.type === 'partitions') {
        resp = resp.map((v: string) => [i, v] as Partition);
      }
      for (const [j, v] of resp.entries()) {
        ret[inArgs[i].indecies[j]] = v;
      }
    }

    return ret;
  }
  async releaseCache(id: number) {
    const { storageType, partitions } = this.caches[id];
    let inArgs = this.workers.map(v => [] as string[]);

    for (let [wid, pid] of partitions) {
      inArgs[wid].push(pid);
    }
    const works = [];
    for (const [i, worker] of this.workers.entries()) {
      works.push(
        worker.processRequest({
          type: workerActions.RELEASE,
          payload: {
            storageType,
            partitions: inArgs[i],
          },
        }),
      );
    }
    await Promise.all(works);
    delete this.caches[id];
  }

  joinPieces(
    numPartitions: number,
    parts: string[][],
    out: OutArgs | OutArgs[],
    mappers: SerializedFunction<(arg: any, partitionIndex: number) => any>[],
    task: TaskDetail,
  ): Promise<any> {
    // projection parts from [workers][newPartition] to [newPartition][workers]
    // and skip null parts.
    parts = new Array(numPartitions)
      .fill(0)
      .map((v, i) => parts.map(v => v[i]).filter(v => v));

    if (!Array.isArray(out) && Array.isArray(out.args)) {
      out = this.splitByWorker(out.args).map(
        args =>
          ({
            ...out,
            args,
          } as OutArgs),
      );
    }

    const indecies = this.getSplitIndecies(numPartitions);
    const partitionParts = this.splitByWorker(parts);

    return this.finalWork(
      partitionParts.map(
        v =>
          ({
            type: 'parts',
            parts: v,
          } as InArgs),
      ),
      indecies,
      out,
      mappers,
      task,
      numPartitions,
    );
  }

  async getPartitionCount(dependWork: Request<any>): Promise<number> {
    const { type, payload } = dependWork;
    switch (type) {
      case masterActions.CREATE_RDD:
      case masterActions.REPARTITION:
      case masterActions.COALESCE:
      case masterActions.SORT:
        return payload.numPartitions;
      case masterActions.LOAD_CACHE:
        const id: number = payload;
        return this.caches[id].partitions.length;
      case masterActions.CONCAT: {
        const subRequests = payload as Request<any>[];
        return (await Promise.all(
          subRequests.map(v => this.getPartitionCount(v)),
        )).reduce((a, b) => a + b);
      }
      case masterActions.LOAD_FILE: {
        const { baseUrl, recursive } = payload;
        const loader = await this.getFileLoader(baseUrl, 'load');
        const files = await loader.listFiles(baseUrl, recursive);
        return files.length;
      }
      case masterActions.MAP:
        return this.getPartitionCount(payload.subRequest);
      default:
        throw new Error(`Unknown request type ${type}`);
    }
  }

  async getFileLoader(
    baseUrl: string,
    type: 'load' | 'save',
  ): Promise<FileLoader> {
    for (const loader of this.fileLoaderRegistry) {
      if (await loader.canHandleUrl(baseUrl, type)) {
        return loader;
      }
    }
    throw new Error(`No valid loader for url ${baseUrl}`);
  }

  async loadFile(
    baseUrl: string,
    recursive: boolean,
    out: OutArgs | OutArgs[],
    mappers: SerializedFunction<(arg: any, partitionIndex: number) => any>[],
    task: TaskDetail,
  ): Promise<any[]> {
    const loader = await this.getFileLoader(baseUrl, 'load');
    const files = await loader.listFiles(baseUrl, recursive);
    const func = loader.createDataLoader(baseUrl);
    mappers.unshift(
      serialize(
        filename =>
          Promise.all([
            Promise.resolve(func(filename)).then(v => [filename, v]),
          ]),
        { func, baseUrl },
      ),
    );
    return this.createRDD(files, out, mappers, task);
  }

  getTaskDetail(
    dependWork: Request<any>,
    out: TaskDetail = { total: 0, index: 0 },
  ) {
    const { type, payload } = dependWork;
    switch (type) {
      case masterActions.CREATE_RDD:
      case masterActions.LOAD_CACHE:
      case masterActions.LOAD_FILE: {
        out.total += 1;
        break;
      }
      case masterActions.CONCAT: {
        const subRequests = payload as any[];
        for (const request of subRequests) {
          this.getTaskDetail(request, out);
        }
        break;
      }
      case masterActions.MAP: {
        const { subRequest } = payload;
        this.getTaskDetail(subRequest, out);
        break;
      }
      case masterActions.REPARTITION:
      case masterActions.COALESCE: {
        const { subRequest } = payload;
        out.total += 1;
        this.getTaskDetail(subRequest, out);
        break;
      }
      case masterActions.SORT: {
        const { subRequest } = payload;
        out.total += 2;
        if (subRequest.type !== masterActions.LOAD_CACHE) {
          out.total += 1;
        }
        this.getTaskDetail(subRequest, out);
        break;
      }
    }
    return out;
  }

  async runWork(
    dependWork: Request<any>,
    out: OutArgs | OutArgs[],
    mappers: SerializedFunction<
      (arg: any, partitionIndex: number) => any
    >[] = [],
    task: TaskDetail = this.getTaskDetail(dependWork),
  ): Promise<any[]> {
    const { type, payload } = dependWork;

    switch (type) {
      case masterActions.CREATE_RDD: {
        const { creator, numPartitions, args } = payload;
        mappers.unshift(creator);
        return this.createRDD(
          new Array(numPartitions).fill(0).map((v, i) => args[i]),
          out,
          mappers,
          task,
        );
      }

      case masterActions.LOAD_CACHE: {
        const id: number = payload;
        return this.loadCache(id, out, mappers, task);
      }

      case masterActions.LOAD_FILE: {
        const { baseUrl, recursive } = payload;
        return this.loadFile(baseUrl, recursive, out, mappers, task);
      }

      case masterActions.CONCAT: {
        const subRequests = payload as any[];
        if (!Array.isArray(out) && Array.isArray(out.args)) {
          // split out args for subrequests.
          let index = 0;
          let newOut: OutArgs[] = [];

          for (const subRequest of subRequests) {
            const partitions = await this.getPartitionCount(subRequest);

            const subOut: OutArgs = {
              ...out,
              args: out.args.slice(index, index + partitions),
            };
            index += partitions;
            newOut.push(subOut);
          }
          out = newOut;
        }
        const resps: any[][] = await Promise.all(
          subRequests.map((v: Request<any>) =>
            this.runWork(v, out, [...mappers], task),
          ),
        );
        return ([] as any).concat(...resps);
      }

      case masterActions.MAP: {
        const { subRequest, func } = payload;
        mappers.unshift(func);
        return this.runWork(subRequest, out, mappers, task);
      }

      case masterActions.REPARTITION: {
        const { subRequest, numPartitions, partitionFunc } = payload;

        // step 1: splice pieces;
        let pieces: string[][] = await this.runWork(
          subRequest,
          {
            type: 'parts',
            partitionFunc,
          },
          [],
          task,
        );

        // Step 2: join pieces and go on.
        return this.joinPieces(numPartitions, pieces, out, mappers, task);
      }

      case masterActions.COALESCE: {
        const { subRequest, numPartitions } = payload;
        const originPartitions = await this.getPartitionCount(subRequest);

        // Arg format(for example 4 pieces):
        // startIndex, [rate0, rate1, rate2]
        const partitionArgs: any[] = [];
        let last: number[] = [];
        partitionArgs.push([0, last]);
        const rate = originPartitions / numPartitions;

        let counter = 0;
        for (let i = 0; i < numPartitions - 1; i++) {
          counter += rate;
          while (counter >= 1) {
            counter -= 1;
            last = [];
            partitionArgs.push([i, last]);
          }
          last.push(counter);
        }
        // manually add last partition to avoid precsion loss.
        while (partitionArgs.length < originPartitions) {
          partitionArgs.push([numPartitions - 1, []]);
        }
        const partitionFunc = serialize(
          (v: any[], arg: [number, number[]]): any => {
            const ret: any[][] = [];
            let lastIndex = 0;
            for (let i = 0; i < arg[0]; i++) {
              ret.push([]);
            }
            for (const rate of arg[1]) {
              const nextIndex = Math.floor(v.length * rate);
              ret.push(v.slice(lastIndex, nextIndex));
              lastIndex = nextIndex;
            }
            ret.push(v.slice(lastIndex));
            return ret;
          },
        );
        const tmp = this.splitByWorker(partitionArgs);

        // step 1: splice pieces;
        let pieces: string[][] = await this.runWork(
          subRequest,
          tmp.map(
            v =>
              ({
                type: 'parts',
                partitionFunc,
                args: v,
              } as OutArgs),
          ),
          [],
          task,
        );

        // Step 2: join pieces and go on.
        return this.joinPieces(numPartitions, pieces, out, mappers, task);
      }

      case masterActions.SORT: {
        const { subRequest, ascending, numPartitions, keyFunc } = payload;
        let cacheId;

        // Step 1: Maybe cache
        if (subRequest.type !== masterActions.LOAD_CACHE) {
          // Auto cache with disk storage.
          // TODO: use MEMORY_OR_DISK instead.
          const storageType = 'disk';
          const partitions = await this.runWork(subRequest, {
            type: 'partitions',
            storageType,
          });
          cacheId = this.addCache(storageType, partitions);
        } else {
          cacheId = subRequest.payload;
        }

        let originRequest = {
          type: masterActions.LOAD_CACHE,
          payload: cacheId,
        };

        // Step 2: sample with 1/n fraction where n is origin partition count.
        // So we get a sample count near to a single partition.
        // Sample contains partitionIndex & localIndex to avoid performance issue
        //   when dealing with too many same values.
        let originPartitionNum = await this.getPartitionCount(originRequest);

        const samples: [any, number, number][] = ([] as any[]).concat(
          ...(await this.runWork(
            originRequest,
            {
              type: 'reduce',
            },
            [
              serialize(
                (v: any[], partitionId: number) => {
                  const ret = [];
                  for (let i = 0; i < v.length; i++) {
                    if (Math.random() * originPartitionNum < 1) {
                      ret.push([keyFunc(v[i]), partitionId, i]);
                    }
                  }
                  return ret;
                },
                {
                  keyFunc,
                  originPartitionNum,
                },
              ),
            ],
          )),
        );

        // Step 3: sort samples, and get seperate points.
        samples.sort((a, b) => {
          if (a[0] !== b[0]) {
            return a[0] < b[0] ? -1 : 1;
          }
          if (a[1] !== b[1]) {
            return a[1] < b[1] ? -1 : 1;
          }
          return a[2] < b[2] ? -1 : 1;
        });

        // get n-1 points in samples.
        const points: [any, number, number][] = [];
        let p = samples.length / numPartitions;
        for (let i = 1; i < numPartitions; i++) {
          let idx = Math.floor(p * i);
          if (idx < points.length) {
            points.push(samples[idx]);
          }
        }

        // Step 4: Repartition
        let pieces: string[][] = await this.runWork(
          originRequest,
          {
            type: 'parts',
            partitionFunc: serialize(
              (v: any[], arg: any, partitionIndex: number) => {
                const ret: any[][] = [];
                for (let i = 0; i < numPartitions; i++) {
                  ret.push([]);
                }
                const tmp = v.map((item, i) => [keyFunc(item), i]);
                tmp.sort((a, b) => {
                  if (a[0] !== b[0]) {
                    return a[0] < b[0] ? -1 : 1;
                  }
                  return a[1] < b[1] ? -1 : 1;
                });
                let index = 0;
                for (const [key, i] of tmp) {
                  // compare with points
                  for (; index < points.length; ) {
                    if (key < points[index][0]) {
                      break;
                    }
                    if (key > points[index][0]) {
                      index++;
                      continue;
                    }
                    if (partitionIndex < points[index][1]) {
                      break;
                    }
                    if (partitionIndex > points[index][1]) {
                      index++;
                      continue;
                    }
                    if (i < points[index][2]) {
                      break;
                    }
                    index++;
                  }
                  ret[index].push([v[i], partitionIndex, i]);
                }
                return ret;
              },
              {
                keyFunc,
                numPartitions,
                points,
              },
            ),
          },
          [],
          task,
        );

        // Release temporary cache.
        if (subRequest.type !== masterActions.LOAD_CACHE) {
          // Release auto cache.
          await this.releaseCache(cacheId);
        }

        // Step 5: Sort in partition.
        // Maybe reverse (if ascending == false).
        mappers.unshift(
          serialize(
            (v: any[]) => {
              const tmp = v.map((item, i) => [
                keyFunc(item[0]),
                item[1],
                item[2],
                i,
              ]);
              tmp.sort((a, b) => {
                if (a[0] !== b[0]) {
                  return a[0] < b[0] ? -1 : 1;
                }
                if (a[1] !== b[1]) {
                  return a[1] < b[1] ? -1 : 1;
                }
                return a[2] < b[2] ? -1 : 1;
              });
              const ret = tmp.map(item => v[item[3]][0]);
              if (!ascending) {
                ret.reverse();
              }
              return ret;
            },
            {
              keyFunc,
              ascending,
            },
          ),
        );
        const ret = await this.joinPieces(
          numPartitions,
          pieces,
          out,
          mappers,
          task,
        );
        if (!ascending) {
          ret.reverse();
        }

        return ret;
      }

      default:
        throw new Error(`Unknown request type ${type}`);
    }
  }
}
