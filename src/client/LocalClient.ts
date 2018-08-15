import { LocalMaster } from '../master/LocalMaster';
import { BaseClient, Request, Response } from './Client';

export class LocalClient extends BaseClient {
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
}
