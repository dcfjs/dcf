import { MasterOpts } from './../master/LocalMaster';
import { LocalMaster } from '../master/LocalMaster';
import { Client, Request, Response, ResponseMessage } from './Client';
import * as os from 'os';
import * as ProgressBar from 'progress';

const showProgress = process.env['DCF_PROGRESS'] !== '0';

export class LocalClient implements Client {
  master: LocalMaster;
  progress: ProgressBar | null = null;

  constructor(opts?: MasterOpts) {
    this.master = new LocalMaster(this, opts);
  }

  init(): Promise<void> {
    return this.master.init();
  }
  dispose(): Promise<void> {
    return this.master.dispose();
  }
  async request<T>(m: Request<T>): Promise<ResponseMessage<any>> {
    const ret = await this.master.processRequest(m);
    if (this.progress) {
      this.progress.terminate();
    }
    this.progress = null;
    return ret;
  }
  workerCount() {
    return this.master.workers.length;
  }

  pauseProgress() {
    if (this.progress) {
      const progress: any = this.progress;
      progress.stream.clearLine();
      progress.stream.cursorTo(0);
    }
  }
  resumeProgress() {
    if (this.progress) {
      const progress: any = this.progress;
      progress.stream.write(progress.lastDraw);
    }
  }

  processMessage(m: Response) {
    switch (m.type) {
      case 'task': {
        if (this.progress) {
          this.progress.terminate();
        }
        if (showProgress) {
          this.progress = new ProgressBar(
            `Task ${m.taskIndex}/${
              m.tasks
            } :percent [:bar] Partition :current/:total :rate/s :etas`,
            {
              total: m.partitions,
              width: 30,
            },
          );
        }
        break;
      }
      case 'progress': {
        if (this.progress) {
          this.progress.tick(m.tick);
        }
        break;
      }
    }
  }
}
