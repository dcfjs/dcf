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
  type: 'reduce' | 'partitions' | 'parts';
  storageType?: StorageType;
  finalReducer?: SerializeFunction<(results: any[]) => any>;
  partitionFunc?: SerializeFunction<(v: any[], arg: any) => any[][]>;
  partitionArgs?: any[];
};

type CacheRecord = {
  storageType: StorageType;
  partitions: string[][];
};

export class MasterServer {
  workers: WorkerClient[] = [];
  caches: CacheRecord[] = [];
  cacheIdCounter: number = 0;

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

  async finalWork(
    ins: InArgs[],
    out: OutArgs | OutArgs[],
    mappers: SerializeFunction<(arg: any) => any>[],
  ): Promise<any> {
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
    return await Promise.all(works);
  }

  createRDD(
    args: any[],
    out: OutArgs | OutArgs[],
    mappers: SerializeFunction<(arg: any) => any>[],
  ): Promise<any> {
    const partitionArgs = this.splitByWorker(args);

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

  async addCache(storageType: StorageType, partitions: string[][]) {
    const id = ++this.cacheIdCounter;
    this.caches[id] = {
      storageType,
      partitions,
    };
    return id;
  }

  loadCache(
    id: number,
    out: OutArgs | OutArgs[],
    mappers: SerializeFunction<(arg: any) => any>[],
  ): Promise<any> {
    const { storageType, partitions } = this.caches[id];
    return this.finalWork(
      partitions.map(
        v =>
          ({
            type: 'partitions',
            partitions: v,
          } as InArgs),
      ),
      out,
      mappers,
    );
  }
  async releaseCache(id: number) {
    const { storageType, partitions } = this.caches[id];
    const works = [];
    for (const [i, worker] of this.workers.entries()) {
      works.push(
        worker.processRequest({
          type: workerActions.RELEASE,
          payload: {
            storageType,
            partitions: partitions[i],
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

  getPartitionCount(dependWork: Request<any>): number {
    const { type, payload } = dependWork;
    switch (type) {
      case masterActions.CREATE_RDD:
      case masterActions.REPARTITION:
      case masterActions.COALESCE:
        return payload.numPartitions;
      case masterActions.MAP:
        return this.getPartitionCount(payload.subRequest);
      default:
        throw new Error(`Unknown request type ${type}`);
    }
  }

  async runWork(
    dependWork: Request<any>,
    out: OutArgs | OutArgs[],
    mappers: SerializeFunction<(arg: any) => any>[] = [],
  ): Promise<any> {
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
          partitionArgs: [],
        });

        // Step 2: join pieces and go on.
        return this.joinPieces(numPartitions, pieces, out, mappers);
      }

      case masterActions.COALESCE: {
        const { subRequest, numPartitions } = payload;
        const originPartitions = this.getPartitionCount(subRequest);

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
                partitionArgs: v,
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
