import { StorageType } from './../common/types';
import { PartId } from './../worker/handlers';
import { MasterServer } from './MasterServer';
import { registerHandler } from '../common/handler';
import { Request } from '../client/Client';
import { SerializeFunction, deserialize } from '../common/SerializeFunction';

export const CREATE_RDD = '@@master/createRDD';
export const MAP = '@@master/map';
export const REDUCE = '@@master/reduce';
export const REPARTITION = '@@master/repartition';
export const COALESCE = '@@master/coalesce';

export const CACHE = '@@master/cache';
export const LOAD_CACHE = '@@master/loadCache';
export const RELEASE_CACHE = '@@master/releaseCache';

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

registerHandler(
  CACHE,
  async (
    {
      subRequest,
      storageType,
    }: {
      subRequest: Request<any>;
      storageType: StorageType;
    },
    context: MasterServer,
  ) => {
    const partitions = await context.runWork(subRequest, {
      type: 'partitions',
      storageType,
    });

    return context.addCache(storageType, partitions);
  },
);

registerHandler(RELEASE_CACHE, (id: number, context: MasterServer) => {
  return context.releaseCache(id);
});
