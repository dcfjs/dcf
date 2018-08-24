import { SerializeFunction, deserialize } from '../common/SerializeFunction';
import { registerHandler } from '../common/handler';
import { StorageType } from '../common/types';
import { promises as fs } from 'fs';
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
  ({ id, mode: _mode }: { id: string; mode: WorkerMode }) => {
    wid = id;
    mode = _mode;
    console.log(`Worker ${wid} inited.`);
  },
);

registerHandler(EXIT, () => {
  console.log(`Worker ${wid} exited`);
  process.exit(0);
});

const partitions: { [key: string]: any[] } = {};
let idCounter = 0;

function saveNewPartition<T>(data: T[]) {
  const id = `rdd-${++idCounter}`;
  partitions[id] = data;
  return id;
}

function getPartitionData<T>(id: string): T[] {
  return partitions[id];
}

function releasePartition(id: string) {
  delete partitions[id];
}

async function createRepartitionPart() {
  const id = `tmp/part-${wid}-${++idCounter}.part`;
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
  return ([] as T[]).concat(...ret);
}

// V[] | PARTITION | PARTS => V | PARTITION | PARTS
registerHandler(
  CALC,
  async ({
    in: { type: inType, args, partitions, parts },
    mappers,
    out: {
      type: outType,
      storageType,
      finalReducer,
      partitionFunc,
      partitionArgs,
    },
  }: {
    in: {
      type: 'value' | 'partitions' | 'parts';
      args: any[];
      partitions: string[];
      parts: string[][];
    };
    mappers: SerializeFunction<(arg: any) => any>[];
    out: {
      type: 'reduce' | 'partitions' | 'parts';
      storageType: StorageType;
      finalReducer: SerializeFunction<(results: any[]) => any>;
      partitionFunc: SerializeFunction<(v: any[], arg: any) => any[][]>;
      partitionArgs: any[];
    };
  }) => {
    const results: any[] = [];
    const funcs = mappers.map(v => deserialize(v));

    const doPartition = (outType === 'parts' && deserialize(partitionFunc)) as (
      v: any[],
      arg: any,
    ) => any[][];

    let index = 0;
    async function work(partition: any) {
      let ret = partition;
      for (const func of funcs) {
        ret = func(ret);
      }

      switch (outType) {
        case 'reduce': {
          results.push(ret);
          break;
        }
        case 'partitions': {
          results.push(await saveNewPartition(ret));
          break;
        }
        case 'parts': {
          const tmp = doPartition(ret, partitionArgs[index++]);
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
      }
    }
    switch (inType) {
      case 'value': {
        for (const arg of args) {
          await work(arg);
        }
        break;
      }
      case 'partitions': {
        for (const id of partitions) {
          await work(getPartitionData(id));
        }
        break;
      }
      case 'parts': {
        for (const part of parts) {
          const pieces = await Promise.all(
            part.map(v => getRepartitionPart(v)),
          );
          let v: any = ([] as any).concat(...pieces);
          await work(v);
        }
        break;
      }
    }
    if (outType === 'reduce') {
      return deserialize(finalReducer)(results);
    }
    return results;
  },
);

registerHandler(RELEASE, (ids: string[]) => {
  for (const id of ids) {
    releasePartition(id);
  }
});
