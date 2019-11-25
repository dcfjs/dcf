import {
  SerializedFunction,
  deserialize,
  setRequireWhiteList,
} from '../common/SerializeFunction';
import { registerHandler } from '../common/handler';
import { StorageType } from '../common/types';
import debug from '../common/debug';
import WorkerContext from './WorkerContext';
import concatArrays from '../common/concatArrays';
import getTmpFolderPath from '../common/getTmpFolderPath';

const fs = require('fs-promise');
const v8 = require('v8');

export const INIT = '@@worker/init';
export const EXIT = '@@worker/exit';

export const CALC = '@@worker/calc';
export const CREATE_PARTITION = '@@worker/createPartition';
export const MAP = '@@worker/map';
export const REDUCE = '@@worker/reduce';
export const RELEASE = '@@worker/release';

export const REPARTITION_SLICE = '@@worker/repartitionSlice';
export const REPARTITION_JOIN = '@@worker/repartitionJoin';
export const REPARTITION_REDUCE = '@@worker/repartitionJoin';
export const REPARTITION_RELEASE = '@@worker/repartitionRelease';

export type WorkerMode = 'network' | 'local';

let wid = 'NO-ID'; // workerId
let mode: WorkerMode = 'local';

registerHandler(
  INIT,
  ({
    id,
    mode: _mode,
    requireWhiteList,
  }: {
    id: string;
    mode: WorkerMode;
    requireWhiteList?: string[];
  }) => {
    wid = id;
    mode = _mode;
    if (requireWhiteList) {
      setRequireWhiteList(requireWhiteList);
    }

    debug(`Worker inited.`);
  },
);

registerHandler(EXIT, () => {
  process.exit(0);
});

const partitions: { [key: string]: any[] } = {};
let idCounter = 0;

async function saveNewPartition<T>(storageType: StorageType, data: T[]) {
  const id = `rdd-${++idCounter}`;
  switch (storageType) {
    case 'disk': {
      const buf = v8.serialize(data);
      await fs.writeFile(`${getTmpFolderPath()}/${wid}-${id}.partition`, buf);
      break;
    }
    case 'memory':
      partitions[id] = data;
      break;
    default:
      throw new Error(`Unsupported storageType ${storageType}`);
  }
  return id;
}

async function getPartitionData<T>(
  storageType: StorageType,
  id: string,
): Promise<T[]> {
  switch (storageType) {
    case 'disk': {
      const buf = await fs.readFile(`${getTmpFolderPath()}/${wid}-${id}.partition`);
      return v8.deserialize(buf);
    }
    case 'memory':
      return partitions[id];
    default:
      throw new Error(`Unsupported storageType ${storageType}`);
  }
}

async function releasePartition(
  storageType: StorageType,
  id: string,
): Promise<void> {
  switch (storageType) {
    case 'disk': {
      await fs.unlink(`${getTmpFolderPath()}/${wid}-${id}.partition`);
      break;
    }
    case 'memory': {
      delete partitions[id];
      break;
    }
    default:
      throw new Error(`Unsupported storageType ${storageType}`);
  }
}

async function createRepartitionPart() {
  const id = `${getTmpFolderPath()}/part-${wid}-${++idCounter}.part`;
  await fs.writeFile(id, Buffer.alloc(0));
  return id;
}

export type PartId = string;

async function appendRepartitionPart<T>(id: PartId, data: T[]) {
  const buf = v8.serialize(data);
  const length = Buffer.alloc(4);
  length.writeInt32LE(buf.length, 0);

  await fs.appendFile(id, Buffer.concat([length, buf]));
}

async function getRepartitionPart<T>(id: PartId): Promise<T[]> {
  const buf = await fs.readFile(id);
  const ret: T[][] = [];
  for (let index = 0; index < buf.length; ) {
    const length = buf.readInt32LE(index);
    ret.push(v8.deserialize(buf.slice(index + 4, index + 4 + length)));
    index += length + 4;
  }
  await fs.unlink(id);
  return concatArrays(ret);
}

// Iterator an array with a async function, and break promise chain to keep memory safe.
function safeRepeat<T>(
  arr: T[],
  func: (arg: T, index: number) => void | Promise<void>,
) {
  return new Promise((resolve, reject) => {
    let index = -1;

    function next() {
      index++;
      if (index >= arr.length) {
        resolve();
        return;
      }
      Promise.resolve(func(arr[index], index)).then(next, reject);
    }
    next();
  });
}

// V[] | PARTITION | PARTS => V | PARTITION | PARTS
registerHandler(
  CALC,
  async (
    {
      in: { type: inType, args, partitions, parts, storageType: inStorageType },
      indecies,
      mappers,
      out: {
        type: outType,
        storageType,
        partitionFunc,
        args: outArgs,
        saveFunc,
      },
    }: {
      in: {
        type: 'value' | 'partitions' | 'parts';
        args: any[];
        storageType: StorageType;
        partitions: string[];
        parts: string[][];
      };
      indecies: number[];
      mappers: SerializedFunction<(arg: any, partitionIndex: number) => any>[];
      out: {
        type: 'reduce' | 'partitions' | 'parts' | 'saveFile';
        storageType: StorageType;
        partitionFunc: SerializedFunction<
          (v: any[], arg: any, partitionIndex: number) => any[][]
        >;
        saveFunc: SerializedFunction<
          (data: any[], filename: string) => void | Promise<void>
        >;
        args: any[];
      };
    },
    context: WorkerContext,
  ) => {
    const results: any[] = [];
    const funcs = mappers.map(v => deserialize(v));

    const doPartition = (outType === 'parts' && deserialize(partitionFunc)) as (
      v: any[],
      arg: any,
      partitinIndex: number,
    ) => any[][];

    const doSave = (outType === 'saveFile' && deserialize(saveFunc)) as ((
      data: any[],
      filename: string,
    ) => void | Promise<void>);

    async function work(partition: any, index: number) {
      let ret = partition;

      const partitionIndex = indecies[index];

      await safeRepeat(funcs, async func => {
        ret = await func(ret, partitionIndex);
      });

      switch (outType) {
        case 'reduce': {
          results.push(ret);
          break;
        }
        case 'partitions': {
          results.push(await saveNewPartition(storageType, ret));
          break;
        }
        case 'parts': {
          const tmp = doPartition(
            ret,
            outArgs && outArgs[index],
            partitionIndex,
          );
          await Promise.all(
            tmp.map(async (v, j) => {
              if (!v || v.length === 0) {
                return;
              }
              let id = results[j];
              if (id == null) {
                id = results[j] = await createRepartitionPart();
              }
              return appendRepartitionPart(id, tmp[j]);
            }),
          );
          break;
        }
        case 'saveFile': {
          await doSave(ret, outArgs[index]);
        }
      }

      context.tick();
    }
    switch (inType) {
      case 'value': {
        await safeRepeat(args, work);
        break;
      }
      case 'partitions': {
        await safeRepeat(partitions, async (id, index) => {
          await work(await getPartitionData(inStorageType, id), index);
        });
        break;
      }
      case 'parts': {
        await safeRepeat(parts, async (part, index) => {
          const pieces = await Promise.all(
            part.map(v => getRepartitionPart(v)),
          );
          let v: any = concatArrays(pieces);
          await work(v, index);
        });
        break;
      }
    }
    return results;
  },
);

registerHandler(
  RELEASE,
  async ({
    storageType,
    partitions,
  }: {
    storageType: StorageType;
    partitions: string[];
  }) => {
    await Promise.all(partitions.map(id => releasePartition(storageType, id)));
  },
);
