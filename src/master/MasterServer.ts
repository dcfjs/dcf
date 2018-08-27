import { StorageType } from './../common/types';
import { WorkerClient } from '../worker/WorkerClient';
import { Request } from '../client/Client';
import { SerializeFunction, serialize } from '../common/SerializeFunction';

import * as workerActions from '../worker/handlers';
import * as masterActions from './handlers';

type InArgs = {
  type: 'value' | 'partitions' | 'parts';
  args?: any[];
  partitions?: string[];
  parts?: string[][];
};

type OutArgs = {
  type: 'reduce' | 'partitions' | 'parts' | 'saveFile';
  storageType?: StorageType;
  saveFunc?: SerializeFunction<
    (data: any[], filename: string) => void | Promise<void>
  >;
  partitionFunc?: SerializeFunction<(v: any[], arg: any) => any[][]>;
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
  ): SerializeFunction<(filename: string) => Buffer | Promise<Buffer>>;

  initSaveProgress(baseUrl: string, overwrite?: boolean): void | Promise<void>;
  createDataSaver(
    baseUrl: string,
  ): SerializeFunction<
    (filename: string, buffer: Buffer) => void | Promise<void>
  >;
  markSaveSuccess(baseUrl: string): void | Promise<void>;
}

export class MasterServer {
  workers: WorkerClient[] = [];
  caches: CacheRecord[] = [];
  cacheIdCounter: number = 0;

  fileLoaderRegistry: FileLoader[] = [];

  registerFileLoader(loader: FileLoader) {
    this.fileLoaderRegistry.push(loader);
  }

  async init(): Promise<void> {}
  async dispose(): Promise<void> {}
  processRequest<T>(m: Request<T>): Promise<any> {
    throw new Error('Should be implemented.');
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
    out: OutArgs | OutArgs[],
    mappers: SerializeFunction<(arg: any) => any>[],
  ): Promise<any[]> {
    const works: Promise<any>[] = [];
    for (const [i, worker] of this.workers.entries()) {
      works.push(
        worker.processRequest({
          type: workerActions.CALC,
          payload: {
            in: ins[i],
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
    mappers: SerializeFunction<(arg: any) => any>[],
  ): Promise<any> {
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
      out,
      mappers,
    );
  }

  async addCache(storageType: StorageType, partitions: Partition[]) {
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
    mappers: SerializeFunction<(arg: any) => any>[],
  ): Promise<any> {
    const { storageType, partitions } = this.caches[id];

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
            mappers,
            out: Array.isArray(out) ? out[i] : out,
          },
        }),
      );
    }
    const resps = await Promise.all(works);

    const ret = partitions.map(v => null);

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
    mappers: SerializeFunction<(arg: any) => any>[],
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

    const partitionParts = this.splitByWorker(parts);

    return this.finalWork(
      partitionParts.map(
        v =>
          ({
            type: 'parts',
            parts: v,
          } as InArgs),
      ),
      out,
      mappers,
    );
  }

  async getPartitionCount(dependWork: Request<any>): Promise<number> {
    const { type, payload } = dependWork;
    switch (type) {
      case masterActions.CREATE_RDD:
      case masterActions.REPARTITION:
      case masterActions.COALESCE:
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
    mappers: SerializeFunction<(arg: any) => any>[],
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
    return this.createRDD(files, out, mappers);
  }

  async runWork(
    dependWork: Request<any>,
    out: OutArgs | OutArgs[],
    mappers: SerializeFunction<(arg: any) => any>[] = [],
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
        );
      }

      case masterActions.LOAD_CACHE: {
        const id: number = payload;
        return this.loadCache(id, out, mappers);
      }

      case masterActions.LOAD_FILE: {
        const { baseUrl, recursive } = payload;
        return this.loadFile(baseUrl, recursive, out, mappers);
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
            this.runWork(v, out, [...mappers]),
          ),
        );
        return ([] as any).concat(...resps);
      }

      case masterActions.MAP: {
        const { subRequest, func } = payload;
        mappers.unshift(func);
        return this.runWork(subRequest, out, mappers);
      }

      case masterActions.REPARTITION: {
        const { subRequest, numPartitions, partitionFunc } = payload;
        // step 1: splice pieces;
        let pieces: string[][] = await this.runWork(subRequest, {
          type: 'parts',
          partitionFunc,
        });

        // Step 2: join pieces and go on.
        return this.joinPieces(numPartitions, pieces, out, mappers);
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
        );

        // Step 2: join pieces and go on.
        return this.joinPieces(numPartitions, pieces, out, mappers);
      }

      default:
        throw new Error(`Unknown request type ${type}`);
    }
  }
}
