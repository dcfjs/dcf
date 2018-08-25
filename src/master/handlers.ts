import { StorageType } from './../common/types';
import { MasterServer } from './MasterServer';
import { registerHandler } from '../common/handler';
import { Request } from '../client/Client';
import {
  SerializeFunction,
  deserialize,
  serialize,
} from '../common/SerializeFunction';

export const CREATE_RDD = '@@master/createRDD';
export const MAP = '@@master/map';
export const REDUCE = '@@master/reduce';
export const REPARTITION = '@@master/repartition';
export const COALESCE = '@@master/coalesce';
export const CONCAT = '@@master/concat';
export const LOAD_FILE = '@@master/loadFile';
export const GET_NUM_PARTITIONS = '@@master/getNumPartitions';
export const SAVE_FILE = '@@master/saveFile';

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
      },
      [partitionFunc],
    );

    return deserialize(finalFunc)(results);
  },
);

registerHandler(
  SAVE_FILE,
  async <T, T1>(
    {
      subRequest,
      baseUrl,
      overwrite = true,
    }: {
      subRequest: Request<any>;
      baseUrl: string;
      overwrite?: boolean;
    },
    context: MasterServer,
  ) => {
    const fileLoader = await context.getFileLoader(baseUrl, 'save');
    await fileLoader.initSaveProgress(baseUrl, overwrite);
    const extension = 'txt';
    const numPartitions = await context.getPartitionCount(subRequest);
    const args = new Array(numPartitions)
      .fill(0)
      .map(
        (v, i) => `part-${('000000' + i.toString(16)).substr(-6)}.${extension}`,
      );

    const saver = fileLoader.createDataSaver(baseUrl);

    const saveFunc = serialize(
      (data: any[], filename: string) => {
        const lines = data.map(v => v.toString()).join('\n');
        const buffer = Buffer.from(lines);
        saver(filename, buffer);
      },
      {
        saver,
      },
    );

    await context.runWork(subRequest, {
      type: 'saveFile',
      saveFunc,
      args,
    });

    await fileLoader.markSaveSuccess(baseUrl);
  },
);

registerHandler(
  GET_NUM_PARTITIONS,
  (subRequest: Request<any>, context: MasterServer) => {
    return context.getPartitionCount(subRequest);
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
