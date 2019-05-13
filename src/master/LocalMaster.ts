import { WorkerOpts } from './../worker/LocalWorker';
import { LocalClient } from './../client/LocalClient';
import { Request, Response } from '../client/Client';
import { LocalWorker } from '../worker/LocalWorker';
import * as os from 'os';
import './handlers';
import { MasterServer } from './MasterServer';
const fs = require('fs-promise');
import * as fileLoader from './loaders/fileLoader';
import debug from '../common/debug';

export interface MasterOpts {
  workerCount?: number;
  worker?: WorkerOpts;
  showProgress?: boolean;
}

export class LocalMaster extends MasterServer {
  client: LocalClient;
  opts: MasterOpts;
  constructor(client: LocalClient, opts: MasterOpts) {
    super();
    this.client = client;
    this.opts = opts;
    this.workers = new Array(this.opts.workerCount)
      .fill(0)
      .map((v, i) => new LocalWorker(this, `${i}`));
    this.registerFileLoader(fileLoader);
  }
  send(m: Response) {
    this.client.processMessage(m);
  }
  async init(): Promise<void> {
    super.init();
    debug('Launching workers.');
    try {
      await fs.mkdirs('tmp');
    } catch (e) {}
    await Promise.all(this.workers.map(v => v.init()));
    debug('Master ready.');
  }
  async dispose(): Promise<void> {
    super.dispose();
    await Promise.all(this.workers.map(v => v.dispose()));
    try {
      const files = await fs.readdir('tmp');
      for (const file of files) {
        await fs.unlink(`tmp/${file}`);
      }
      await fs.rmdir('tmp');
    } catch (e) {}
    debug('Bye.');
  }
}
