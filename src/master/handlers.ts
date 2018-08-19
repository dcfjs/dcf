import { MasterServer } from './MasterServer';
import { registerHandler } from '../common/handler';
import { Request } from '../client/Client';
import {
  SerializeFunction,
  deserialize,
  serialize,
} from '../common/SerializeFunction';
import * as workerActions from '../worker/handlers';
import { WorkerClient } from '../worker/WorkerClient';
import { PartitionType } from '../common/types';

export const CREATE_RDD = '@@master/createRDD';
export const MAP = '@@master/map';
export const REDUCE = '@@master/reduce';
export const REPARTITION = '@@master/REPARTITION';

export const LOAD_CACHE = '@@master/loadCache';

class Partition {
  worker: WorkerClient;
  id: string;

  constructor(worker: WorkerClient, id: string) {
    this.worker = worker;
    this.id = id;
  }
}

type TaskRecord = {
  worker: WorkerClient;
  ids: string[];
  indecies: number[];
};

function groupByWorker(partitions: Partition[]) {
  const ret: TaskRecord[] = [];
  const map: { [key: string]: TaskRecord } = {};
  let index = 0;
  for (const partition of partitions) {
    const { worker } = partition;
    let record;
    if (!map[worker.id]) {
      record = {
        worker,
        ids: [],
        indecies: [],
      };
      map[worker.id] = record;
      ret.push(record);
    } else {
      record = map[worker.id];
    }
    record.ids.push(partition.id);
    record.indecies.push(index++);
  }
  return ret;
}

function flatWorkerResult(tasks: TaskRecord[], resp: any[][]) {
  const results = [];
  for (let i = 0; i < tasks.length; i++) {
    const result = resp[i] as any[];
    const task = tasks[i];
    for (let j = 0; j < result.length; j++) {
      results[task.indecies[j]] = result[j];
    }
  }
  return results;
}

registerHandler(
  CREATE_RDD,
  async (
    {
      numPartitions,
      creator,
      args,
      type,
    }: {
      numPartitions?: number;
      creator: SerializeFunction;
      args: any[];
      type: PartitionType;
    },
    context: MasterServer,
  ) => {
    const workerCount = context.workers.length;
    numPartitions = numPartitions == null ? workerCount : numPartitions;
    const rest = numPartitions % workerCount;
    const eachCount = (numPartitions - rest) / workerCount;

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
  MAP,
  async (
    {
      subRequest,
      func,
    }: {
      subRequest: Request;
      func: SerializeFunction;
    },
    context: MasterServer,
  ) => {
    const subPartitions: Partition[] = await context.processRequest(subRequest);
    const tasks = groupByWorker(subPartitions);

    const partitions = flatWorkerResult(
      tasks,
      await Promise.all(
        tasks.map(v =>
          v.worker.processRequest({
            type: workerActions.MAP,
            payload: {
              func,
              ids: v.ids,
            },
          }),
        ),
      ),
    ).map((v, i) => new Partition(subPartitions[i].worker, v));

    if (subRequest.type !== LOAD_CACHE) {
      await Promise.all(
        tasks.map(v =>
          v.worker.processRequest({
            type: workerActions.RELEASE,
            payload: v.ids,
          }),
        ),
      );
    }
    return partitions;
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

    const tasks = groupByWorker(partitions);

    const results = flatWorkerResult(
      tasks,
      await Promise.all(
        tasks.map(v =>
          v.worker.processRequest({
            type: workerActions.REDUCE,
            payload: {
              func: partitionFunc,
              ids: v.ids,
            },
          }),
        ),
      ),
    );

    if (subRequest.type !== LOAD_CACHE) {
      await Promise.all(
        tasks.map(v =>
          v.worker.processRequest({
            type: workerActions.RELEASE,
            payload: v.ids,
          }),
        ),
      );
    }
    const func = deserialize(finalFunc);
    return func(results);
  },
);

registerHandler(
  REPARTITION,
  async (
    {
      subRequest,
      numPartitions,
      partitionFunc,
    }: {
      subRequest: Request;
      numPartitions: number;
      partitionFunc: SerializeFunction;
    },
    context: MasterServer,
  ) => {
    const workerCount = context.workers.length;
    numPartitions = numPartitions == null ? workerCount : numPartitions;

    const partitions: Partition[] = await context.processRequest(subRequest);
    const tasks = groupByWorker(partitions);

    // Step1: split each partitions to numPartitions parts, and collect their information
    // Local mode: return a local file name.
    // Remote mode: return a rdd id & worker host & port.
    // Empty part will return null.
    let pieces = await Promise.all(
      tasks.map(v =>
        v.worker.processRequest({
          type: workerActions.REPARTITION_SLICE,
          payload: {
            ids: v.ids,
            numPartitions,
            partitionFunc: serialize(
              (data: any[]) => {
                const ret: any[][] = new Array(numPartitions)
                  .fill(0)
                  .map(v => []);
                for (const item of data) {
                  const id = (partitionFunc as any)(item);
                  ret[id].push(item);
                }
                return ret;
              },
              {
                numPartitions,
                partitionFunc,
              },
            ),
          },
        }),
      ),
    );

    // Release dependencies
    if (subRequest.type !== LOAD_CACHE) {
      await Promise.all(
        tasks.map(v =>
          v.worker.processRequest({
            type: workerActions.RELEASE,
            payload: v.ids,
          }),
        ),
      );
    }

    // projection pieces from [workers][newPartition] to [newPartition][workers]
    // and skip null parts.
    pieces = new Array(numPartitions)
      .fill(0)
      .map((v, i) => pieces.map(v => v[i]).filter(v => v));

    // Step2: regenerate new partitions. and release parts.
    const rest = numPartitions % workerCount;
    const eachCount = (numPartitions - rest) / workerCount;

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
            type: workerActions.REPARTITION_JOIN,
            payload: pieces.slice(partitionIndex, partitionIndex + subCount),
          });
          return ids.map((id: string) => new Partition(worker, id));
        })(),
      );
      partitionIndex += subCount;
    }
    return ([] as any[]).concat(...(await Promise.all(result)));
  },
);
