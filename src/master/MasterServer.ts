import { WorkerClient } from '../worker/WorkerClient';
import { Request } from '../client/Client';

export class MasterServer {
  workers: WorkerClient[] = [];

  async init(): Promise<void> {}
  async dispose(): Promise<void> {}
  processRequest<T>(m: Request<T>): Promise<any> {
    throw new Error('Should be implemented.');
  }
}
