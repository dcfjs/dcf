import { Request } from '../client';
import { processRequest } from '../common/handler';
import './handlers';

if (!process.send) {
  throw new Error('This entry must be loaded by local master!');
}

const queue: Request<any>[] = [];

async function processNextRequest() {
  try {
    const m = await processRequest(queue[0]);
    if (process.send) {
      process.send({
        ok: true,
        payload: m,
      });
    }
  } catch (e) {
    console.warn(e.stack);
    if (process.send) {
      process.send({
        ok: false,
        message: e.message,
      });
    }
  }
  queue.shift();
  if (queue.length > 0) {
    processNextRequest();
  }
}

process.on('message', <T>(m: Request<T>) => {
  queue.push(m);
  if (queue.length === 1) {
    processNextRequest();
  }
});
