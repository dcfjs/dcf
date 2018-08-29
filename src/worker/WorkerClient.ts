import { Request } from '../client/Client';

export class WorkerClient {
  id: string;
  debug: (msg: string, ...args: any[]) => void;

  constructor(id: string) {
    this.id = id;
    this.debug = require('debug')(`dcf:worker:${id}`);
  }
  init() {
    throw new Error('Must be overriden.');
  }
  processRequest<T>(m: Request<T>): any | Promise<any> {
    throw new Error('Must be overriden.');
  }
  dispose(): void | Promise<void> {
    throw new Error('Must be overriden.');
  }
}
