import { Request, Response } from '../client';
import { processRequest } from '../common/handler';
import './handlers';
import { setDebugFunc } from '../common/debug';
import WorkerContext from './WorkerContext';
const v8 = require('v8');
const { send: _send } = process;

if (!_send) {
  throw new Error('This entry must be loaded by local master!');
}

const send = function(msg: Response) {
  _send.call(process, v8.serialize(msg).toString('base64'));
};

setDebugFunc((msg, ...args) => {
  send({
    type: 'debug',
    msg,
    args,
  });
});

class LocalWorkerContext extends WorkerContext {
  send(msg: Response) {
    send(msg);
  }
}

const context = new LocalWorkerContext();

const queue: Request<any>[] = [];

async function processNextRequest() {
  try {
    const m = await processRequest(queue[0], context);
    send({
      type: 'resp',
      ok: true,
      payload: m,
    });
  } catch (e) {
    console.log(e);
    send({
      type: 'resp',
      ok: false,
      message: e.message,
    });
  }
  queue.shift();
  if (queue.length > 0) {
    processNextRequest();
  }
}

process.on('message', <T>(buf: string) => {
  const m: Request<T> = v8.deserialize(Buffer.from(buf, 'base64'));
  queue.push(m);
  if (queue.length === 1) {
    processNextRequest();
  }
});
