import * as cp from 'child_process';
import { Request, Response } from '../client';
import { INIT, EXIT } from './handlers';
import { WorkerClient } from './WorkerClient';
import { LocalMaster } from '../master/LocalMaster';
const v8 = require('v8');

const entryScript = require.resolve('./localEntry');
const isType = /\.ts$/.test(entryScript);

export interface WorkerOpts {
  nodeArgs?: string[];
}

function createWorker(opts: WorkerOpts) {
  return cp.fork(entryScript, [], {
    env: process.env,
    stdio: 'inherit',
    execArgv: opts.nodeArgs || [],
  });
}

const defaultOpts: WorkerOpts = {
  nodeArgs: isType ? ['-r', 'ts-node/register'] : [],
};

export class LocalWorker extends WorkerClient {
  worker: cp.ChildProcess | null = null;
  sequence: Function[][] = [];
  waitExit?: Function;
  master: LocalMaster;
  opts: WorkerOpts;

  constructor(master: LocalMaster, id: string, opts?: WorkerOpts) {
    super(id);
    this.master = master;
    this.opts = {
      ...defaultOpts,
      ...opts,
    };
  }

  async init(): Promise<void> {
    if (this.worker != null) {
      throw new Error('Worker already inited.');
    }
    this.worker = createWorker(this.opts);
    this.worker.on('message', this.onMessage);
    this.worker.on('exit', this.onExit);
    process.on('exit', this.onProcessExit);
    await this.processRequest({
      type: INIT,
      payload: {
        id: this.id,
        mode: 'local',
      },
    });
  }
  sendToWorker(m: Request<any>) {
    if (this.worker) {
      this.worker.send(v8.serialize(m).toString('base64'));
    }
  }
  onMessage = (buf: string) => {
    const r = v8.deserialize(Buffer.from(buf, 'base64'));
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
        // Pause progress bar and print debug message.
        this.master.client.pauseProgress();
        this.debug(r.msg, ...r.args);
        this.master.client.resumeProgress();
        break;
      }
      case 'progress': {
        this.master.send(r);
        break;
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
    this.worker = null;
  };
  processRequest<T>(m: Request<T>): Promise<any> {
    return new Promise((resolve, reject) => {
      if (this.worker == null) {
        throw new Error('Worker not inited.');
      }
      this.sequence.push([resolve, reject]);
      this.sendToWorker(m);
    });
  }
  async dispose() {
    await new Promise(resolve => {
      if (this.worker == null) {
        throw new Error('Worker not inited.');
      }
      this.waitExit = resolve;
      // Do not wait for response.
      this.sendToWorker({
        type: EXIT,
      });
    });
    this.debug('Exited successfully.');
    process.removeListener('exit', this.onProcessExit);
  }
  onProcessExit = () => {
    // This is a non-gracefully case that parent process exited before worker exited, manually kill all worker
    // while on some platform the child process will hang.
    if (this.worker) {
      this.worker.kill('SIGKILL');
    }
  };
}
