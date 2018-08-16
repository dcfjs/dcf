import { Request } from '../client/Client';

export interface WorkerClient {
  id: string;
  init(): Promise<void>;
  processRequest(m: Request): Promise<any>;
  dispose(): Promise<void>;
}
