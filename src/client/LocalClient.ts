import { LocalMaster } from '../master/LocalMaster';
import { Client, Request, Response } from './Client';
import * as os from 'os';

export class LocalClient implements Client {
  master: LocalMaster;

  constructor(workerCount: number = os.cpus().length) {
    this.master = new LocalMaster(workerCount);
  }

  init(): Promise<void> {
    return this.master.init();
  }
  dispose(): Promise<void> {
    return this.master.dispose();
  }
  request<T>(m: Request<T>): Promise<Response<any>> {
    return this.master.processRequest(m);
  }
  workerCount() {
    return this.master.workers.length;
  }
}
