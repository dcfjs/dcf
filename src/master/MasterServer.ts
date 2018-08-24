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

    let partitionPartitionArgs: any = null;

    if (out.type === 'parts') {
      partitionPartitionArgs = this.splitByWorker(out.partitionArgs as any[]);
    }

    const works: Promise<any>[] = [];
    for (const [i, worker] of this.workers.entries()) {
      let tmpOut = out;
      if (tmpOut.type === 'parts') {
        tmpOut = {
          ...out,
          partitionArgs: partitionPartitionArgs[i],
        };
      }
      works.push(
        worker.processRequest({
          type: workerActions.CALC,
          payload: {
            in: {
              type: 'value',
              args: partitionArgs[i],
            } as InArgs,
            mappers,
            out: tmpOut,
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
        // step 1: splice pieces;
        let pieces: string[][] = await this.runWork(subRequest, {
          type: 'parts',
          partitionFunc: serialize(
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
              console.log(v, arg, ret);
              return ret;
            },
          ),
          partitionArgs,
        });

        // Step 2: join pieces and go on.
        return this.joinPieces(numPartitions, pieces, out, mappers);
      }

      default:
        throw new Error(`Unknown request type ${type}`);
    }
  }
}
