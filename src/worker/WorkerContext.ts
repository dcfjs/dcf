import { Response } from './../client/Client';

export default class WorkerContext {
  send(msg: Response) {
    throw new Error('Must be implemented.');
  }
  tick(tick = 1) {
    this.send({
      type: 'progress',
      tick,
    });
  }
}
