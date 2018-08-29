import { StorageType } from './../common/types';
import {
  FunctionEnv,
  requireModule,
  SerializeFunction,
} from './../common/SerializeFunction';
import {
  REDUCE,
  CREATE_RDD,
  MAP,
  REPARTITION,
  COALESCE,
  LOAD_CACHE,
  RELEASE_CACHE,
  CACHE,
  CONCAT,
  LOAD_FILE,
  GET_NUM_PARTITIONS,
  SAVE_FILE,
} from './../master/handlers';
import { Client, Request } from './Client';
import { serialize } from '../common/SerializeFunction';
const XXHash = require('xxhash');
const v8 = require('v8');

type ResponseFactory<T> = (rdd: RDD<T>) => Request<any> | Promise<Request<any>>;

function hashPartitionFunc<V>(numPartitions: number) {
  const seed = ((Math.random() * 0xffffffff) | 0) >>> 0;
  return serialize(
    (data: V) => XXHash.hash(v8.serialize(data), seed) % numPartitions,
    {
      numPartitions,
      seed,
      XXHash: requireModule('xxhash'),
      v8: requireModule('v8'),
    },
  );
}

export class RDD<T> {
  context: Context;
  constructor(context: Context) {
    this.context = context;
  }

  generateTask(): Request<any> | Promise<Request<any>> {
    throw new Error('Must be overrided.');
  }

  async collect(): Promise<T[][]> {
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(),
        partitionFunc: serialize((data: T[]) => data),
        finalFunc: serialize((results: T[][]) => {
          return ([] as any).concat(...results);
        }),
      },
    });
  }
  async take(count: number): Promise<T[]> {
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(),
        partitionFunc: serialize((data: T[]) => data.slice(0, count), {
          count,
        }),
        finalFunc: serialize(
          (results: T[][]) => {
            let total = 0;
            const ret: T[][] = [];
            for (const result of results) {
              if (total + result.length >= count) {
                ret.push(result.slice(0, count - total));
                break;
              }
              ret.push(result);
              total += result.length;
            }
            return ([] as T[]).concat(...ret);
          },
          { count },
        ),
      },
    });
  }
  async count(): Promise<number> {
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(),
        partitionFunc: serialize((data: T[]) => data.length),
        finalFunc: serialize((result: number[]) =>
          result.reduce((a, b) => a + b, 0),
        ),
      },
    });
  }
  async max(): Promise<number | null> {
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(),
        partitionFunc: serialize((data: (T | null)[]) =>
          data.reduce(
            (a, b) => (a !== null && (b === null || a > b) ? a : b),
            null,
          ),
        ),
        finalFunc: serialize((result: (T | null)[]) =>
          result.reduce(
            (a, b) => (a !== null && (b === null || a > b) ? a : b),
            null,
          ),
        ),
      },
    });
  }
  async min(): Promise<number> {
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(),
        partitionFunc: serialize((data: (T | null)[]) =>
          data.reduce(
            (a, b) => (a !== null && (b === null || a < b) ? a : b),
            null,
          ),
        ),
        finalFunc: serialize((result: (T | null)[]) =>
          result.reduce(
            (a, b) => (a !== null && (b === null || a < b) ? a : b),
            null,
          ),
        ),
      },
    });
  }
  mapPartitions<T1>(
    func: (v: T[]) => T1[] | Promise<T1[]>,
    env?: FunctionEnv,
  ): RDD<T1> {
    if (typeof func === 'function') {
      func = serialize(func, env);
    }
    const generateTask = async () => ({
      type: MAP,
      payload: {
        subRequest: await this.generateTask(),
        func,
      },
    });
    return new GeneratedRDD<T1>(this.context, generateTask);
  }
  glom() {
    return this.mapPartitions((v: T[]) => [v]);
  }
  map<T1>(func: ((v: T) => T1), env?: FunctionEnv): RDD<T1> {
    if (typeof func === 'function') {
      func = serialize(func, env);
    }
    return this.mapPartitions((partition: T[]) => partition.map(func), {
      func,
    });
  }
  async reduce(func: ((a: T, b: T) => T), env?: FunctionEnv): Promise<number> {
    if (typeof func === 'function') {
      func = serialize(func, env);
    }
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(),
        partitionFunc: serialize(
          (data: T[]) => (data.length > 0 ? [data.reduce(func)] : []),
          {
            func,
          },
        ),
        finalFunc: serialize(
          (result: T[][]) => {
            const arr = ([] as T[]).concat(...result);
            return arr.length > 0 ? arr.reduce(func) : null;
          },
          {
            func,
          },
        ),
      },
    });
  }
  flatMap<T1>(func: ((v: T) => T1[]), env?: FunctionEnv): RDD<T1> {
    if (typeof func === 'function') {
      func = serialize(func, env);
    }
    return this.mapPartitions(
      (partition: T[]) => ([] as T1[]).concat(...partition.map(func)),
      {
        func,
      },
    );
  }
  filter(func: (v: T) => boolean, env?: FunctionEnv): RDD<T> {
    if (typeof func === 'function') {
      func = serialize(func, env);
    }

    return this.mapPartitions((partition: T[]) => partition.filter(func), {
      func,
    });
  }
  repartition(numPartitions: number): RDD<T> {
    return this.partitionBy(
      numPartitions,
      serialize(() => (Math.random() * numPartitions) | 0, { numPartitions }),
    );
  }
  partitionBy(
    numPartitions: number,
    partitionFunc: (v: T) => number,
    env?: FunctionEnv,
  ) {
    if (typeof partitionFunc === 'function') {
      partitionFunc = serialize(partitionFunc, env);
    }

    const finalPartitionFunc = serialize(
      (data: any[]) => {
        const ret: any[][] = new Array(numPartitions).fill(0).map(v => []);
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
    );

    const generateTask = async () => ({
      type: REPARTITION,
      payload: {
        subRequest: await this.generateTask(),
        numPartitions,
        partitionFunc: finalPartitionFunc,
      },
    });
    return new GeneratedRDD<T>(this.context, generateTask);
  }
  coalesce(numPartitions: number) {
    const generateTask = async () => ({
      type: COALESCE,
      payload: {
        subRequest: await this.generateTask(),
        numPartitions,
      },
    });
    return new GeneratedRDD<T>(this.context, generateTask);
  }
  reduceByKey<K, V>(
    this: RDD<[K, V]>,
    func: ((a: V, B: V) => V),
    numPartitions: number = this.context.client.workerCount(),
    partitionFunc?: (v: K) => number,
    env?: FunctionEnv,
  ): RDD<[K, V]> {
    return this.combineByKey(
      x => x,
      func,
      func,
      numPartitions,
      partitionFunc,
      env,
    );
  }
  combineByKey<K, V, C>(
    this: RDD<[K, V]>,
    createCombiner: ((a: V) => C),
    mergeValue: ((a: C, b: V) => C),
    mergeCombiners: ((a: C, b: C) => C),
    numPartitions: number = this.context.client.workerCount(),
    partitionFunc: (v: K) => number = hashPartitionFunc<K>(numPartitions),
    env?: FunctionEnv,
  ): RDD<[K, C]> {
    if (typeof createCombiner === 'function') {
      createCombiner = serialize(createCombiner, env);
    }
    if (typeof mergeValue === 'function') {
      mergeValue = serialize(mergeValue, env);
    }
    if (typeof mergeCombiners === 'function') {
      mergeCombiners = serialize(mergeCombiners, env);
    }
    if (typeof partitionFunc === 'function') {
      partitionFunc = serialize(partitionFunc, env);
    }

    const mapFunction1 = serialize(
      (datas: [K, V][]) => {
        const ret = [];
        const map: { [key: string]: [K, C] } = {};
        for (const item of datas) {
          const k = v8.serialize(item[0]).toString('base64');
          let r = map[k];
          if (!r) {
            r = [item[0], createCombiner(item[1])];
            map[k] = r;
            ret.push(r);
          } else {
            r[1] = mergeValue(r[1], item[1]);
          }
        }
        return ret;
      },
      {
        createCombiner,
        mergeValue,
        v8: requireModule('v8'),
      },
    );

    const mapFunction2 = serialize(
      (datas: [K, C][]) => {
        const ret = [];
        const map: { [key: string]: [K, C] } = {};
        for (const item of datas) {
          const k = v8.serialize(item[0]).toString('base64');
          let r = map[k];
          if (!r) {
            r = [item[0], item[1]];
            map[k] = r;
            ret.push(r);
          } else {
            r[1] = mergeCombiners(r[1], item[1]);
          }
        }
        return ret;
      },
      {
        mergeCombiners,
        v8: requireModule('v8'),
      },
    );

    const realPartitionFunc = serialize(
      (data: [K, C]) => {
        return partitionFunc(data[0]);
      },
      {
        partitionFunc,
      },
    );

    return this.mapPartitions<[K, C]>(mapFunction1)
      .partitionBy(numPartitions, realPartitionFunc)
      .mapPartitions<[K, C]>(mapFunction2);
  }
  persist(storageType: StorageType = 'memory'): CacheRDD<T> {
    return new CacheRDD(storageType, this);
  }
  cache(): CacheRDD<T> {
    return this.persist('memory');
  }
  union(...others: RDD<T>[]): RDD<T> {
    return this.context.union(this, ...others);
  }

  async getNumPartitions(): Promise<number> {
    return this.context.client.request({
      type: GET_NUM_PARTITIONS,
      payload: await this.generateTask(),
    });
  }

  async saveAsTextFile(
    baseUrl: string,
    {
      overwrite = true,
      encoding = 'utf8',
      extension = 'txt',

      compressor,
      functionEnv,
    }: {
      overwrite?: boolean;
      encoding?: string;
      extension?: string;

      compressor?: (data: Buffer) => Buffer | Promise<Buffer>;
      functionEnv?: FunctionEnv;
    } = {},
  ): Promise<void> {
    if (typeof compressor === 'function') {
      compressor = serialize(compressor, functionEnv);
    }

    return this.context.client.request({
      type: SAVE_FILE,
      payload: {
        subRequest: await this.generateTask(),
        baseUrl,
        overwrite,
        extension,
        serializer: serialize(
          async (data: any[]) => {
            const lines = data.map(v => v.toString()).join('\n');
            let buffer = Buffer.from(lines, encoding);
            if (compressor) {
              buffer = await compressor(buffer);
            }

            return buffer;
          },
          {
            encoding,
            compressor,
          },
        ),
      },
    });
  }
}

export class GeneratedRDD<T> extends RDD<T> {
  _generateTask: ResponseFactory<T>;

  constructor(context: Context, generateTask: ResponseFactory<T>) {
    super(context);
    this.context = context;
    this._generateTask = generateTask;
  }

  generateTask(): Request<any> | Promise<Request<any>> {
    return this._generateTask(this);
  }
}

export class CacheRDD<T> extends RDD<T> {
  cacheId: number | null = null;
  dependency: RDD<T>;
  storageType: StorageType;
  constructor(storageType: StorageType, dependency: RDD<T>) {
    super(dependency.context);
    this.storageType = storageType;
    this.dependency = dependency;
  }
  async generateTask(): Promise<Request<any>> {
    if (this.cacheId == null) {
      this.cacheId = await this.context.client.request({
        type: CACHE,
        payload: {
          storageType: this.storageType,
          subRequest: await this.dependency.generateTask(),
        },
      });
    }
    return {
      type: LOAD_CACHE,
      payload: this.cacheId,
    };
  }
  async unpersist(): Promise<void> {
    if (this.cacheId != null) {
      const { cacheId } = this;
      this.cacheId = null;
      await this.context.client.request({
        type: RELEASE_CACHE,
        payload: cacheId,
      });
    }
  }
}

export class Context {
  client: Client;
  constructor(c: Client) {
    this.client = c;
  }

  emptyRDD(): RDD<never> {
    return new GeneratedRDD<never>(this, () => ({
      type: CREATE_RDD,
      payload: {
        partitionCount: 0,
      },
    }));
  }

  range(
    from: number,
    to?: number,
    step: number = 1,
    numPartitions?: number,
  ): RDD<number> {
    if (to == null) {
      to = from;
      from = 0;
    }
    numPartitions = numPartitions || this.client.workerCount();
    const finalCount = Math.ceil((to - from) / step);

    const rest = finalCount % numPartitions;
    const eachCount = (finalCount - rest) / numPartitions;

    interface Arg {
      from: number;
      count: number;
    }

    const args: Arg[] = [];
    let index = 0;
    for (let i = 0; i < numPartitions; i++) {
      const subCount = i < rest ? eachCount + 1 : eachCount;
      const end = index + subCount;
      args.push({
        from: from + step * index,
        count: subCount,
      });
      index = end;
    }

    return new GeneratedRDD<number>(this, () => ({
      type: CREATE_RDD,
      payload: {
        numPartitions,
        creator: serialize(
          ({ from, count }: Arg) => {
            const ret = [];
            for (let i = 0; i < count; i++) {
              ret.push(from + step * i);
            }
            return ret;
          },
          { step },
        ),
        args,
        type: 'memory',
      },
    }));
  }

  parallelize<T>(arr: T[], numPartitions?: number): RDD<T> {
    numPartitions = numPartitions || this.client.workerCount();
    const args: T[][] = [];

    const rest = arr.length % numPartitions;
    const eachCount = (arr.length - rest) / numPartitions;

    let index = 0;
    for (let i = 0; i < numPartitions; i++) {
      const subCount = i < rest ? eachCount + 1 : eachCount;
      const end = index + subCount;
      args.push(arr.slice(index, end));
      index = end;
    }

    return new GeneratedRDD<T>(this, () => ({
      type: CREATE_RDD,
      payload: {
        numPartitions,
        creator: serialize((arg: T[]) => arg),
        args,
        type: 'memory',
      },
    }));
  }

  union<T>(...rdds: RDD<T>[]): RDD<T> {
    return new GeneratedRDD<T>(this, async () => ({
      type: CONCAT,
      payload: await Promise.all(rdds.map(v => v.generateTask())),
    }));
  }

  binaryFiles(
    baseUrl: string,
    {
      recursive = false,
    }: {
      recursive?: boolean;
    } = {},
  ): RDD<[string, Buffer]> {
    return new GeneratedRDD<[string, Buffer]>(this, () => ({
      type: LOAD_FILE,
      payload: {
        baseUrl,
        recursive,
      },
    }));
  }

  wholeTextFiles(
    baseUrl: string,
    {
      decompressor,
      encoding = 'utf8',
      recursive = false,
      functionEnv,
    }: {
      encoding?: string;
      recursive?: boolean;

      decompressor?: (data: Buffer) => Buffer | Promise<Buffer>;
      functionEnv?: FunctionEnv;
    } = {},
  ): RDD<[string, string]> {
    if (typeof encoding === 'boolean') {
      recursive = encoding;
      encoding = 'utf-8';
    }
    if (typeof decompressor === 'function') {
      decompressor = serialize(decompressor, functionEnv);
    }
    return this.binaryFiles(baseUrl, { recursive }).mapPartitions(
      async v => {
        let buf = v[0][1];
        if (decompressor) {
          buf = await decompressor(buf);
        }
        return [[v[0][0], buf.toString(encoding)] as [string, string]];
      },
      { encoding, decompressor },
    );
  }

  textFile(
    baseUrl: string,
    options?: {
      encoding?: string;
      recursive?: boolean;

      decompressor?: (data: Buffer) => Buffer;
      functionEnv?: FunctionEnv;

      __dangerousDontCopy?: boolean;
    },
  ): RDD<string> {
    const { __dangerousDontCopy: dontCopy = false } = options || {};

    return this.wholeTextFiles(baseUrl, options).flatMap(
      v => {
        const ret = v[1].replace(/\\r/m, '').split('\n');
        // Remove last empty line.
        if (!ret[ret.length - 1]) {
          ret.pop();
        }
        if (dontCopy) {
          return ret;
        }
        // Fix memory leak: sliced string keep reference of huge string
        // see https://bugs.chromium.org/p/v8/issues/detail?id=2869
        return ret.map(v => (' ' + v).substr(1));
      },
      {
        dontCopy,
      },
    );
  }
}
