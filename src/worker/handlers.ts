import { SerializeFunction, deserialize } from '../common/SerializeFunction';
import { registerHandler } from '../common/handler';
import { PartitionType } from '../common/types';
import { promises as fs } from 'fs';
const v8 = require('v8');

export const INIT = '@@worker/init';
export const EXIT = '@@worker/exit';

export const CREATE_PARTITION = '@@worker/createPartition';
export const MAP = '@@worker/map';
export const REDUCE = '@@worker/reduce';
export const RELEASE = '@@worker/release';

export const REPARTITION_SLICE = '@@worker/repartitionSlice';
export const REPARTITION_JOIN = '@@worker/repartitionJoin';
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

function saveNewPartition(data: any[]) {
  const id = `rdd-${++idCounter}`;
  partitions[id] = data;
  return id;
}

function getPartitionData(id: string) {
  return partitions[id];
}

function releasePartition(id: string) {
  delete partitions[id];
}

async function createRepartitionPart() {
  const id = `tmp/part-${wid}-${++idCounter}.part`;
  return id;
}

async function appendRepartitionPart(id: any, data: any[]) {
  const buf = v8.serialize(data);
  const length = Buffer.alloc(4);
  length.writeInt32LE(buf.length, 0);

  await fs.appendFile(id, Buffer.concat([length, buf]));
}

async function getRepartitionPart(id: any) {
  const buf = await fs.readFile(id);
  const ret: any[][] = [];
  for (let index = 0; index < buf.length; ) {
    const length = buf.readInt32LE(index);
    ret.push(v8.deserialize(buf.slice(index + 4, index + 4 + length)));
    index += length + 4;
  }
  await fs.unlink(id);
  return ([] as any).concat(...ret);
}

registerHandler(
  CREATE_PARTITION,
  ({
    type,
    creator,
    count,
    args,
  }: {
    type: PartitionType;
    creator: SerializeFunction;
    count: number;
    args: any[];
  }) => {
    const func = deserialize(creator);
    const ret: (string | null)[] = [];
    for (let i = 0; i < count; i++) {
      ret.push(saveNewPartition(func(args[i])));
    }
    return ret;
  },
);

registerHandler(
  MAP,
  ({ ids, func }: { ids: string[]; func: SerializeFunction }) => {
    const f = deserialize(func);
    return ids.map(id => saveNewPartition(f(partitions[id])));
  },
);

registerHandler(
  REDUCE,
  ({ ids, func }: { ids: string[]; func: SerializeFunction }) => {
    const f = deserialize(func);
    return ids.map(id => f(getPartitionData(id)));
  },
);

registerHandler(RELEASE, (ids: string[]) => {
  for (const id of ids) {
    releasePartition(id);
  }
});

registerHandler(
  REPARTITION_SLICE,
  async ({
    ids,
    numPartitions,
    partitionFunc,
  }: {
    ids: string[];
    numPartitions: number;
    partitionFunc: SerializeFunction;
  }) => {
    const func = deserialize(partitionFunc);
    const ret = await Promise.all(new Array(numPartitions).fill(null));
    for (const id of ids) {
      const partition = getPartitionData(id);
      const tmp: any[][] = ret.map(v => []);
      for (const item of partition) {
        const id = func(item);
        tmp[id].push(item);
      }
      await Promise.all(
        ret.map(async (id, i) => {
          if (tmp[i].length === 0) {
            return;
          }
          if (id == null) {
            id = ret[i] = await createRepartitionPart();
          }
          return appendRepartitionPart(id, tmp[i]);
        }),
      );
    }
    return ret;
  },
);

registerHandler(REPARTITION_JOIN, async (partsList: any[][]) => {
  const partitions = [];

  // serial for each new parition, parallel for parts.
  for (const parts of partsList) {
    const datas: any[][] = await Promise.all(
      parts.map(v => getRepartitionPart(v)),
    );

    partitions.push(saveNewPartition(([] as any).concat(...datas)));
  }
  return partitions;
});
