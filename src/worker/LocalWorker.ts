import * as cp from 'child_process';
import { Request, Response } from '../client';
import { INIT, EXIT } from './handlers';

const entryScript = require.resolve('./localEntry');
const isType = /\.ts$/.test(entryScript);

function createWorker() {
  return cp.fork(entryScript, [], {
    env: process.env,
    execArgv: isType ? ['-r', 'ts-node/register'] : [],
  });
}

export class LocalWorker {
  id: string;
  worker: cp.ChildProcess | null = null;
  sequence: Function[][] = [];
  waitExit?: Function;
  constructor(id: string) {
    this.id = id;
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
      payload: this.id,
    });
  }
  onMessage = (r: Response) => {
    const msg = this.sequence.shift();
    if (!msg) {
      console.warn('Unexpected response from worker.');
      return;
    }
    if (r.ok) {
      msg[0](r);
    } else {
      msg[1](new Error(r.payload));
    }
  };
  onExit = (code: number) => {
    if (code !== 0) {
      throw new Error('Worker exit abnormally.');
    }
    if (this.waitExit) {
      this.waitExit();
      this.waitExit = undefined;
    }
  };
  processRequest(m: Request): Promise<any> {
    return new Promise((resolve, reject) => {
      if (this.worker == null) {
        throw new Error('Worker not inited.');
      }
      this.sequence.push([resolve, reject]);
      this.worker.send(m);
    });
  }
  dispose() {
    return new Promise(resolve => {
      if (this.worker == null) {
        throw new Error('Worker not inited.');
      }
      this.waitExit = resolve;
      // Do not wait for response.
      this.worker.send({
        type: EXIT,
      });
    });
  }
}
