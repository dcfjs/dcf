import { MasterServer } from './MasterServer';
import { registerHandler } from '../common/handler';
import { Request } from '../client/Client';
import { SerializeFunction, deserialize } from '../common/SerializeFunction';
import * as workerActions from '../worker/handlers';
import { WorkerClient } from '../worker/WorkerClient';
import { PartitionType } from '../common/types';

export const CREATE_RDD = '@@master/createRDD';
export const REDUCE = '@@master/reduce';

export const LOAD_CACHE = '@@master/loadCache';

class Partition {
  worker: WorkerClient;
  id: string;
  released: boolean = false;

  constructor(worker: WorkerClient, id: string) {
    this.worker = worker;
    this.id = id;
  }
  async reduce(func: SerializeFunction): Promise<any> {
    return this.worker.processRequest({
      type: workerActions.REDUCE,
      payload: {
        func,
        id: this.id,
      },
    });
  }
  release() {}
}

registerHandler(
  CREATE_RDD,
  async (
    {
      partitionCount,
      creator,
      args,
      type,
    }: {
      partitionCount?: number;
      creator: SerializeFunction;
      args: any[];
      type: PartitionType;
    },
    context: MasterServer,
  ) => {
    const workerCount = context.workers.length;
    partitionCount = partitionCount || workerCount;
    const rest = partitionCount % workerCount;
    const eachCount = (partitionCount - rest) / workerCount;

    const result: Promise<Partition[]>[] = [];

    let partitionIndex = 0;

    for (let i = 0; i < workerCount; i++) {
      const subCount = i < rest ? eachCount + 1 : eachCount;
      if (subCount <= 0) {
        break;
      }
      result.push(
        (async (): Promise<Partition[]> => {
          const worker = context.workers[i];
          const ids = await worker.processRequest({
            type: workerActions.CREATE_PARTITION,
            payload: {
              type,
              creator,
              count: subCount,
              args: args.slice(partitionIndex, partitionIndex + subCount),
            },
          });
          return ids.map((id: string) => new Partition(worker, id));
        })(),
      );
      partitionIndex += subCount;
    }

    return ([] as any[]).concat(...(await Promise.all(result)));
  },
);

registerHandler(
  REDUCE,
  async (
    {
      subRequest,
      partitionFunc,
      finalFunc,
    }: {
      subRequest: Request;
      partitionFunc: SerializeFunction;
      finalFunc: SerializeFunction;
    },
    context: MasterServer,
  ) => {
    const partitions: Partition[] = await context.processRequest(subRequest);

    const results = await Promise.all(
      partitions.map(v => v.reduce(partitionFunc)),
    );

    if (subRequest.type !== LOAD_CACHE) {
      await Promise.all(partitions.map(v => v.release()));
    }
    const func = deserialize(finalFunc);
    return func(results);
  },
);
