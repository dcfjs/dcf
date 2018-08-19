import { Request } from '../client/Client';

export interface WorkerClient {
  id: string;
  init(): Promise<void>;
  processRequest<T>(m: Request<T>): Promise<any>;
  dispose(): Promise<void>;
}
