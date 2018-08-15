export interface RDD<T> {}

export interface Client {
  parallelize<T>(arr: Array<T>): RDD<T>;
}

export interface Request {
  type: string;
  payload?: any;
}

export interface Response {
  ok: boolean;
  message?: string;
  payload?: any;
}

export class BaseClient implements Client {
  constructor() {
    if (this.constructor === BaseClient) {
      throw new Error('BaseClient is a absolute class!');
    }
  }
  request(m: Request): Promise<Response> {
    throw new Error('Must be implemented.');
  }
  dispose(): void | Promise<void> {}
  parallelize<T>(arr: Array<T>): RDD<T> {
    throw new Error('TODO');
  }
}
