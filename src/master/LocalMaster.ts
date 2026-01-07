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
import { setRequireWhiteList } from '../common/SerializeFunction';
import getTmpFolderPath from '../common/getTmpFolderPath';

export interface MasterOpts {
  workerCount?: number;
  worker?: WorkerOpts;
  showProgress?: boolean;
  requireWhiteList?: string[];
}

export class LocalMaster extends MasterServer {
  client: LocalClient;
  opts: MasterOpts;
  constructor(client: LocalClient, opts: MasterOpts) {
    super();
    this.client = client;
    this.opts = opts;
    this.workers = new Array(this.opts.workerCount).fill(0).map(
      (v, i) =>
        new LocalWorker(this, `${i}`, {
          requireWhiteList: opts.requireWhiteList,
          ...opts.worker,
        }),
    );
    this.registerFileLoader(fileLoader);
    if (opts.requireWhiteList) {
      setRequireWhiteList(opts.requireWhiteList);
    }
  }
  send(m: Response) {
    this.client.processMessage(m);
  }
  async init(): Promise<void> {
    await super.init();
    debug('Launching workers.');
    try {
      await fs.mkdirs(getTmpFolderPath());
    } catch (e) {}
    await Promise.all(this.workers.map(v => v.init()));
    debug('Master ready.');
  }
  async dispose(): Promise<void> {
    await super.dispose();
    await Promise.all(this.workers.map(v => v.dispose()));
    const tmpFolderPath = getTmpFolderPath();
    try {
      const files = await fs.readdir(tmpFolderPath);
      for (const file of files) {
        await fs.unlink(`${tmpFolderPath}/${file}`);
      }
      await fs.rmdir(tmpFolderPath);
    } catch (e) {}
    debug('Bye.');
  }
}
