import { SerializeFunction, FunctionEnv } from './../common/SerializeFunction';
import { PartitionType } from './../common/types';
import { REDUCE, CREATE_RDD, MAP } from './../master/handlers';
import { Client, Request } from './Client';
import { serialize } from '../common/SerializeFunction';

type ResponseFactory<T> = (rdd: RDD<T>) => Request | Promise<Request>;

class RDD<T> {
  context: Context;
  generateTask: ResponseFactory<T>;

  constructor(context: Context, generateTask: ResponseFactory<T>) {
    this.context = context;
    this.generateTask = generateTask;
  }

  async take(count: number): Promise<T[]> {
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(this),
        partitionFunc: serialize((data: any[]) => data.slice(0, count), {
          count,
        }),
        finalFunc: serialize(
          (results: any[][]) => {
            let total = 0;
            const ret: any[][] = [];
            for (const result of results) {
              if (total + result.length >= count) {
                ret.push(result.slice(0, count - total));
                break;
              }
              ret.push(result);
              total += result.length;
            }
            return ([] as any[]).concat(...ret);
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
        partitionFunc: serialize((data: any[]) => data.length),
        finalFunc: serialize((result: number[]) =>
          result.reduce((a, b) => a + b, 0),
        ),
      },
    });
  }
  async max(): Promise<number> {
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(this),
        partitionFunc: serialize((data: any[]) =>
          data.reduce(
            (a, b) => (a !== null && (b === null || a > b) ? a : b),
            null,
          ),
        ),
        finalFunc: serialize((result: any[]) =>
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
        partitionFunc: serialize((data: any[]) =>
          data.reduce(
            (a, b) => (a !== null && (b === null || a < b) ? a : b),
            null,
          ),
        ),
        finalFunc: serialize((result: any[]) =>
          result.reduce(
            (a, b) => (a !== null && (b === null || a < b) ? a : b),
            null,
          ),
        ),
      },
    });
  }
  map<T1>(
    func: ((v: T) => T1) | SerializeFunction,
    env?: FunctionEnv,
  ): RDD<T1> {
    const serializedFunc =
      typeof func === 'function' ? serialize(func, env) : func;
    const generateTask = async (): Promise<Request> => ({
      type: MAP,
      payload: {
        subRequest: await this.generateTask(this),
        func: serialize((partition: any[]) => partition.map(func as any), {
          func: serializedFunc,
        }),
      },
    });
    return new RDD<T1>(this.context, generateTask);
  }
  filter(
    func: ((v: T) => boolean) | SerializeFunction,
    env?: FunctionEnv,
  ): RDD<T> {
    const serializedFunc =
      typeof func === 'function' ? serialize(func, env) : func;

    const generateTask = async (): Promise<Request> => ({
      type: MAP,
      payload: {
        subRequest: await this.generateTask(this),
        func: serialize((partition: any[]) => partition.filter(func as any), {
          func: serializedFunc,
        }),
      },
    });
    return new RDD<T>(this.context, generateTask);
  }
}

export class Context {
  client: Client;
  constructor(c: Client) {
    this.client = c;
  }

  emptyRDD(): RDD<never> {
    return new RDD<never>(
      this,
      (): Request => ({
        type: CREATE_RDD,
        payload: {
          partitionCount: 0,
        },
      }),
    );
  }

  parallelize<T>(arr: T[], numSlice?: number): RDD<T> {
    numSlice = numSlice || this.client.workerCount();
    const args: T[][] = [];

    const rest = arr.length % numSlice;
    const eachCount = (arr.length - rest) / numSlice;

    let index = 0;
    for (let i = 0; i < numSlice; i++) {
      const subCount = i < rest ? eachCount + 1 : eachCount;
      const end = index + subCount;
      args.push(arr.slice(index, end));
      index = end;
    }

    return new RDD<T>(
      this,
      (): Request => ({
        type: CREATE_RDD,
        payload: {
          partitionCount: numSlice,
          creator: serialize((arg: T[]) => arg),
          args: args,
          type: 'memory',
        },
      }),
    );
  }
}
