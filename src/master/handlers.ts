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

registerHandler(
  REDUCE,
  async <T, T1>(
    {
      subRequest,
      partitionFunc,
      finalFunc,
    }: {
      subRequest: Request<any>;
      partitionFunc: SerializeFunction<(arg: T[]) => T1>;
      finalFunc: SerializeFunction<(arg: T1[]) => T1>;
    },
    context: MasterServer,
  ) => {
    const results: T1[] = await context.runWork(
      subRequest,
      {
        type: 'reduce',
        finalReducer: finalFunc,
      },
      [partitionFunc],
    );

    return deserialize(finalFunc)(results);
  },
);
