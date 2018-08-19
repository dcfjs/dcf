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

async function saveRepartitionPart(data: any[]) {
  const id = `tmp/part-${wid}-${++idCounter}.part`;
  await fs.writeFile(id, v8.serialize(data));
  return id;
}

async function getRepartitionPart(id: any) {
  const ret = v8.deserialize(await fs.readFile(id));
  await fs.unlink(id);
  return ret;
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
  ({
    ids,
    numPartitions,
    partitionFunc,
  }: {
    ids: string[];
    numPartitions: number;
    partitionFunc: SerializeFunction;
  }) => {
    const func = deserialize(partitionFunc);
    return Promise.all(
      ids.map(async id => {
        const partition = partitions[id];
        const parts: any[][] = new Array(numPartitions).fill(0).map(v => []);
        for (const item of partition) {
          const id = func(item);
          parts[id].push(item);
        }
        return Promise.all(parts.map(saveRepartitionPart));
      }),
    );
  },
);

registerHandler(REPARTITION_JOIN, async (partsList: any[][]) => {
  const partitions = [];

  // serial for each new parition, parallel for parts.
  for (const parts of partsList) {
    const datas: any[][] = await Promise.all(parts.map(getRepartitionPart));

    partitions.push(saveNewPartition(([] as any).concat(...datas)));
  }
  return partitions;
});
