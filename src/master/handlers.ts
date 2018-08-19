import { PartId } from './../worker/handlers';
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
export const REPARTITION = '@@master/repartition';
export const COALESCE = '@@master/coalesce';

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

function flatWorkerResult<T>(tasks: TaskRecord[], resp: T[][]) {
  const results: T[] = [];
  for (let i = 0; i < tasks.length; i++) {
    const result = resp[i] as T[];
    const task = tasks[i];
    for (let j = 0; j < result.length; j++) {
      results[task.indecies[j]] = result[j];
    }
  }
  return results;
}

registerHandler(
  CREATE_RDD,
  async <T, Arg>(
    {
      numPartitions,
      creator,
      args,
      type,
    }: {
      numPartitions?: number;
      creator: SerializeFunction<(arg: Arg) => T[]>;
      args: Arg[];
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

    return ([] as Partition[]).concat(...(await Promise.all(result)));
  },
);

registerHandler(
  MAP,
  async <T, T1>(
    {
      subRequest,
      func,
    }: {
      subRequest: Request<any>;
      func: SerializeFunction<(v: T[]) => T1[]>;
    },
    context: MasterServer,
  ) => {
    const subPartitions: Partition[] = await context.processRequest(subRequest);
    const tasks = groupByWorker(subPartitions);

    const partitions = flatWorkerResult(
      tasks,
      await Promise.all(
        tasks.map(
          v =>
            v.worker.processRequest({
              type: workerActions.MAP,
              payload: {
                func,
                ids: v.ids,
              },
            }) as Promise<string[]>,
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
  async <T, T1, T2>(
    {
      subRequest,
      partitionFunc,
      finalFunc,
    }: {
      subRequest: Request<any>;
      partitionFunc: SerializeFunction<(arg: T[]) => T1>;
      finalFunc: SerializeFunction<(arg: T1[]) => T2>;
    },
    context: MasterServer,
  ) => {
    const partitions: Partition[] = await context.processRequest(subRequest);

    const tasks = groupByWorker(partitions);

    const results = flatWorkerResult(
      tasks,
      await Promise.all(
        tasks.map(
          v =>
            v.worker.processRequest({
              type: workerActions.REDUCE,
              payload: {
                func: partitionFunc,
                ids: v.ids,
              },
            }) as Promise<T1[]>,
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
  async <T>(
    {
      subRequest,
      numPartitions,
      partitionFunc,
    }: {
      subRequest: Request<any>;
      numPartitions: number;
      partitionFunc: SerializeFunction<(v: T) => number>;
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
      tasks.map(
        v =>
          v.worker.processRequest({
            type: workerActions.REPARTITION_SLICE,
            payload: {
              ids: v.ids,
              numPartitions,
              partitionFunc: serialize(
                (data: T[]) => {
                  const ret: T[][] = new Array(numPartitions)
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
              args: [],
            },
          }) as Promise<PartId[][]>,
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
    return ([] as Partition[]).concat(...(await Promise.all(result)));
  },
);

registerHandler(
  COALESCE,
  async <T>(
    {
      subRequest,
      numPartitions,
    }: {
      subRequest: Request<any>;
      numPartitions: number;
    },
    context: MasterServer,
  ) => {
    const workerCount = context.workers.length;
    numPartitions = numPartitions == null ? workerCount : numPartitions;

    const partitions: Partition[] = await context.processRequest(subRequest);
    const tasks = groupByWorker(partitions);

    // Step1: get count of each partition.
    const counts = flatWorkerResult(
      tasks,
      await Promise.all(
        tasks.map(
          v =>
            v.worker.processRequest({
              type: workerActions.REDUCE,
              payload: {
                ids: v.ids,
                func: serialize((arr: T[]) => arr.length),
              },
            }) as Promise<number[]>,
        ),
      ),
    );
    const totalCount = counts.reduce((a, b) => a + b, 0);
    const restCount = totalCount % numPartitions;
    const eachPartCount = (totalCount - restCount) / numPartitions;
    // args[partitionId][] = [newPartitionId, sliceStart, sliceCount];
    const args: [number, number, number][][] = [];
    let nextIndex = 0;
    let requireCount =
      nextIndex < restCount ? eachPartCount + 1 : eachPartCount;
    for (let i = 0; i < counts.length; i++) {
      const current: [number, number, number][] = [];
      let currentCount = counts[i];
      let currentIndex = 0;
      for (; currentCount > 0; ) {
        const minCount =
          requireCount < currentCount ? requireCount : currentCount;
        current.push([nextIndex, currentIndex, minCount]);
        requireCount -= minCount;
        currentCount -= minCount;
        currentIndex += minCount;
        if (requireCount <= 0) {
          nextIndex++;
          requireCount =
            nextIndex < restCount ? eachPartCount + 1 : eachPartCount;
        }
      }
      args.push(current);
    }
    // Step2: split each partitions to numPartitions parts, and collect their information
    // Local mode: return a local file name.
    // Remote mode: return a rdd id & worker host & port.
    // Empty part will return null.
    let pieces = await Promise.all(
      tasks.map(
        v =>
          v.worker.processRequest({
            type: workerActions.REPARTITION_SLICE,
            payload: {
              ids: v.ids,
              numPartitions,
              partitionFunc: serialize(
                (data: T[], arg: [number, number, number][]) => {
                  const ret: T[][] = new Array(numPartitions).fill(null);
                  for (const item of arg) {
                    ret[item[0]] = data.slice(item[1], item[1] + item[2]);
                  }
                  return ret;
                },
                {
                  numPartitions,
                },
              ),
              args: v.indecies.map(i => args[i]),
            },
          }) as Promise<string[]>,
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

    // Step3: regenerate new partitions. and release parts.
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
    return ([] as Partition[]).concat(...(await Promise.all(result)));
  },
);
