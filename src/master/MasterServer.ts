import { WorkerClient } from '../worker/WorkerClient';
import { Request } from '../client/Client';
import { SerializeFunction, serialize } from '../common/SerializeFunction';
import { StorageType } from '../common/types';

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

export class MasterServer {
  workers: WorkerClient[] = [];

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

  async createRDD(
    args: any[],
    out: OutArgs,
    mappers: SerializeFunction<(arg: any) => any>[],
  ): Promise<any> {
    const partitionArgs = this.splitByWorker(args);

    const works: Promise<any>[] = [];
    for (const [i, worker] of this.workers.entries()) {
      works.push(
        worker.processRequest({
          type: workerActions.CALC,
          payload: {
            in: {
              type: 'value',
              args: partitionArgs[i],
            } as InArgs,
            mappers,
            out,
          },
        }),
      );
    }
    const results = await Promise.all(works);

    return results;
  }

  async joinPieces(
    numPartitions: number,
    parts: string[][],
    out: OutArgs,
    mappers: SerializeFunction<(arg: any) => any>[],
  ): Promise<any> {
    // projection parts from [workers][newPartition] to [newPartition][workers]
    // and skip null parts.
    parts = new Array(numPartitions)
      .fill(0)
      .map((v, i) => parts.map(v => v[i]).filter(v => v));

    const partitionParts = this.splitByWorker(parts);

    const works: Promise<any>[] = [];
    for (const [i, worker] of this.workers.entries()) {
      works.push(
        worker.processRequest({
          type: workerActions.CALC,
          payload: {
            in: {
              type: 'parts',
              parts: partitionParts[i],
            } as InArgs,
            mappers,
            out,
          },
        }),
      );
    }
    const results = await Promise.all(works);

    return results;
  }

  async runWork(
    dependWork: Request<any>,
    out: OutArgs,
    mappers: SerializeFunction<(arg: any) => any>[] = [],
  ): Promise<any> {
    const { type, payload } = dependWork;

    switch (type) {
      // Final works:
      case masterActions.CREATE_RDD: {
        const { creator, numPartitions, args } = payload;
        mappers.unshift(creator);
        return this.createRDD(
          new Array(numPartitions).fill(0).map((v, i) => args[i]),
          out,
          mappers,
        );
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
          partitionFunc: serialize(
            (data: any[]) => {
              const ret: any[][] = new Array(numPartitions)
                .fill(0)
                .map(v => []);
              for (const item of data) {
                const id = partitionFunc(item);
                ret[id].push(item);
              }
              return ret;
            },
            {
              numPartitions,
              partitionFunc,
            },
          ),
          partitionArgs: [],
        });

        // Step 2: join pieces and go on.
        return this.joinPieces(numPartitions, pieces, out, mappers);
      }

      default:
        throw new Error(`Unknown request type ${type}`);
    }
  }
}
