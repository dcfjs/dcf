import { SerializeFunction, deserialize } from '../common/SerializeFunction';
import { registerHandler } from '../common/handler';
import { PartitionType } from '../common/types';
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

function* generateCalc<Arg, Ret>(
  inputs: Iterable<Arg>,
  serializedFuncs: SerializeFunction<(arg: any) => any>[],
): Iterable<Ret> {
  const funcs = serializedFuncs.map(v => deserialize(v));

  for (const input of inputs) {
    yield funcs.reduce((v, func) => func(v), input);
  }
}

// V => V
registerHandler(
  CALC,
  ({
    funcs,
    args,
  }: {
    funcs: SerializeFunction<(arg: any) => any>;
    args: any[];
  }) => {},
);

// V => Partition
registerHandler(
  CREATE_PARTITION,
  <T, Arg>({
    type,
    creator,
    count,
    args,
  }: {
    type: PartitionType;
    creator: SerializeFunction<(arg: Arg) => T[]>;
    count: number;
    args: Arg[];
  }) => {
    const func = deserialize(creator);
    const ret: (string | null)[] = [];
    for (let i = 0; i < count; i++) {
      ret.push(saveNewPartition(func(args[i])));
    }
    return ret;
  },
);

// Partition => Partition
registerHandler(
  MAP,
  <T, T1>({
    ids,
    func,
  }: {
    ids: string[];
    func: SerializeFunction<(v: T[]) => T1[]>;
  }) => {
    const f = deserialize(func);
    return ids.map(id => saveNewPartition(f(partitions[id])));
  },
);

// Partition => V
registerHandler(
  REDUCE,
  <T, T1>({
    ids,
    func,
  }: {
    ids: string[];
    func: SerializeFunction<(v: T[]) => T1>;
  }) => {
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
  async <T, Arg>({
    ids,
    numPartitions,
    partitionFunc,
    args,
  }: {
    ids: string[];
    numPartitions: number;
    partitionFunc: SerializeFunction<(v: T[], arg: Arg) => T[][]>;
    args: Arg[];
  }) => {
    const func = deserialize(partitionFunc);
    const ret = await Promise.all(new Array(numPartitions).fill(null));
    for (const [i, id] of ids.entries()) {
      const partition = getPartitionData<T>(id);
      const tmp: T[][] = func(partition, args[i]);
      await Promise.all(
        ret.map(async (id, j) => {
          if (!tmp[j] || tmp[j].length === 0) {
            return;
          }
          if (id == null) {
            id = ret[j] = await createRepartitionPart();
          }
          return appendRepartitionPart(id, tmp[j]);
        }),
      );
    }
    return ret;
  },
);

registerHandler(REPARTITION_JOIN, async <T>(partsList: string[][]) => {
  const partitions = [];

  // serial for each new parition, parallel for parts.
  for (const parts of partsList) {
    const datas: T[][] = await Promise.all(
      parts.map(v => getRepartitionPart<T>(v)),
    );

    partitions.push(saveNewPartition(([] as T[]).concat(...datas)));
  }
  return partitions;
});
