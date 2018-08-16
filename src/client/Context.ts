import { PartitionType } from './../common/types';
import { REDUCE, CREATE_RDD } from './../master/handlers';
import { Client, Request } from './Client';
import { serialize } from '../common/SerializeFunction';

type ResponseFactory<T> = (rdd: RDD<T>) => Request | Promise<Request>;

class RDD<T> {
  context: Context;
  generateTask: ResponseFactory<T>;
  dependencies: RDD<any>[];

  constructor(
    context: Context,
    generateTask: ResponseFactory<T>,
    dependencies: RDD<any>[] = [],
  ) {
    this.context = context;
    this.generateTask = generateTask;
    this.dependencies = dependencies;
  }

  async count(): Promise<number> {
    return this.context.client.request({
      type: REDUCE,
      payload: {
        subRequest: await this.generateTask(this),
        partitionFunc: serialize((partition: any[]) => partition.length),
        finalFunc: serialize((result: number[]) =>
          result.reduce((a, b) => a + b),
        ),
      },
    });
  }
}

export class Context {
  client: Client;
  constructor(c: Client) {
    this.client = c;
  }

  parallelize<T>(
    arr: T[],
    {
      type = 'memory',
      partitionCount = this.client.workerCount(),
    }: {
      type?: PartitionType;
      partitionCount?: number;
    } = {},
  ): RDD<T> {
    const args: T[][] = [];

    const rest = arr.length % partitionCount;
    const eachCount = (arr.length - rest) / partitionCount;

    let index = 0;
    for (let i = 0; i < partitionCount; i++) {
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
          partitionCount,
          creator: serialize((arg: T[]) => arg),
          args: args,
          type,
        },
      }),
    );
  }
}
