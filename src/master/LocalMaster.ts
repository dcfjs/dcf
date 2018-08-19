import { Request, Response } from '../client/Client';
import { LocalWorker } from '../worker/LocalWorker';
import * as os from 'os';
import './handlers';
import { processRequest } from '../common/handler';
import { MasterServer } from './MasterServer';
import { promises as fs } from 'fs';

export class LocalMaster extends MasterServer {
  constructor(workerCount: number = os.cpus().length) {
    super();
    this.workers = new Array(workerCount)
      .fill(0)
      .map((v, i) => new LocalWorker(`worker-${i}`));
  }
  async init(): Promise<void> {
    try {
      await fs.mkdir('tmp');
    } catch (e) {}
    await Promise.all(this.workers.map(v => v.init()));
  }
  async dispose(): Promise<void> {
    await Promise.all(this.workers.map(v => v.dispose()));
    try {
      const files = await fs.readdir('tmp');
      for (const file of files) {
        await fs.unlink(`tmp/${file}`);
      }
      await fs.rmdir('tmp');
    } catch (e) {}
  }
  async processRequest(m: Request): Promise<any> {
    return processRequest(m, this);
  }
}
