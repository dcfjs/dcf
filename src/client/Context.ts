import { FunctionEnv, requireModule } from './../common/SerializeFunction';
import {
  REDUCE,
  CREATE_RDD,
  MAP,
  REPARTITION,
  COALESCE,
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

class RDD<T> {
  context: Context;
  generateTask: ResponseFactory<T>;

  constructor(context: Context, generateTask: ResponseFactory<T>) {
    this.context = context;
    this.generateTask = generateTask;
  }

  async collect(): Promise<T[][]> {
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(this),
        partitionFunc: serialize((data: T[]) => [data]),
        finalFunc: serialize((results: T[][][]) => {
          return ([] as any).concat(...results);
        }),
      },
    });
  }
  async take(count: number): Promise<T[]> {
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(this),
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
        subRequest: await this.generateTask(this),
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
        subRequest: await this.generateTask(this),
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
        subRequest: await this.generateTask(this),
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
  mapPartitions<T1>(func: (v: T[]) => T1[], env?: FunctionEnv): RDD<T1> {
    if (typeof func === 'function') {
      func = serialize(func, env);
    }
    const generateTask = async () => ({
      type: MAP,
      payload: {
        subRequest: await this.generateTask(this),
        func,
      },
    });
    return new RDD<T1>(this.context, generateTask);
  }
  map<T1>(func: ((v: T) => T1), env?: FunctionEnv): RDD<T1> {
    if (typeof func === 'function') {
      func = serialize(func, env);
    }
    return this.mapPartitions((partition: T[]) => partition.map(func), {
      func,
    });
  }
  filter(func: (v: T) => boolean, env?: FunctionEnv): RDD<T> {
    if (typeof func === 'function') {
      func = serialize(func, env);
    }

    return this.mapPartitions((partition: T[]) => partition.filter(func), {
      func,
    });
  }
  repartition(numPartitions: number, seed?: number): RDD<T> {
    if (seed == null) {
      seed = ((Math.random() * 0xffffffff) | 0) >>> 0;
    }
    return this.partitionBy(numPartitions, hashPartitionFunc<T>(numPartitions));
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
        subRequest: await this.generateTask(this),
        numPartitions,
        partitionFunc: finalPartitionFunc,
      },
    });
    return new RDD<T>(this.context, generateTask);
  }
  coalesce(numPartitions: number) {
    const generateTask = async () => ({
      type: COALESCE,
      payload: {
        subRequest: await this.generateTask(this),
        numPartitions,
      },
    });
    return new RDD<T>(this.context, generateTask);
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
}

export class Context {
  client: Client;
  constructor(c: Client) {
    this.client = c;
  }

  emptyRDD(): RDD<never> {
    return new RDD<never>(this, () => ({
      type: CREATE_RDD,
      payload: {
        partitionCount: 0,
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

    return new RDD<T>(this, () => ({
      type: CREATE_RDD,
      payload: {
        numPartitions,
        creator: serialize((arg: T[]) => arg),
        args,
        type: 'memory',
      },
    }));
  }
}
