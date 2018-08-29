import * as cp from 'child_process';
import { Request, Response } from '../client';
import { INIT, EXIT } from './handlers';
import { WorkerClient } from './WorkerClient';
import { LocalMaster } from '../master/LocalMaster';

const entryScript = require.resolve('./localEntry');
const isType = /\.ts$/.test(entryScript);

function createWorker() {
  return cp.fork(entryScript, [], {
    env: process.env,
    execArgv: isType ? ['-r', 'ts-node/register'] : [],
  });
}

export class LocalWorker extends WorkerClient {
  worker: cp.ChildProcess | null = null;
  sequence: Function[][] = [];
  waitExit?: Function;
  master: LocalMaster;

  constructor(master: LocalMaster, id: string) {
    super(id);
    this.master = master;
  }

  async init(): Promise<void> {
    if (this.worker != null) {
      throw new Error('Worker already inited.');
    }
    this.worker = createWorker();
    this.worker.on('message', this.onMessage);
    this.worker.on('exit', this.onExit);
    await this.processRequest({
      type: INIT,
      payload: {
        id: this.id,
        mode: 'local',
      },
    });
  }
  onMessage = (r: Response) => {
    switch (r.type) {
      case 'resp': {
        const msg = this.sequence.shift();
        if (!msg) {
          console.warn('Unexpected response from worker.');
          return;
        }
        if (r.ok) {
          msg[0](r.payload);
        } else {
          msg[1](new Error(r.message));
        }
        break;
      }
      case 'debug': {
        this.debug(r.msg, ...r.args);
      }
    }
  };
  onExit = (code: number) => {
    if (code !== 0) {
      // Crash master process
      throw new Error('Worker exit abnormally.');
    }
    if (this.waitExit) {
      this.waitExit();
      this.waitExit = undefined;
    }
  };
  processRequest<T>(m: Request<T>): Promise<any> {
    return new Promise((resolve, reject) => {
      if (this.worker == null) {
        throw new Error('Worker not inited.');
      }
      this.sequence.push([resolve, reject]);
      this.worker.send(m);
    });
  }
  async dispose() {
    await new Promise(resolve => {
      if (this.worker == null) {
        throw new Error('Worker not inited.');
      }
      this.waitExit = resolve;
      // Do not wait for response.
      this.worker.send({
        type: EXIT,
      });
    });
    this.debug('Exited successfully.');
  }
}
