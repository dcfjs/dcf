import { LocalMaster } from '../master/LocalMaster';
import { Client, Request, Response } from './Client';

export class LocalClient implements Client {
  master = new LocalMaster();

  init(): Promise<void> {
    return this.master.init();
  }
  dispose(): Promise<void> {
    return this.master.dispose();
  }
  request(m: Request): Promise<Response> {
    return this.master.processRequest(m);
  }
  workerCount() {
    return this.master.workers.length;
  }
}
