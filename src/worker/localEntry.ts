import { Request } from '../client';
import { processRequest } from '../common/handler';
import './handlers';

if (!process.send) {
  throw new Error('This entry must be loaded by local master!');
}

const queue: Request[] = [];

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

process.on('message', (m: Request) => {
  queue.push(m);
  if (queue.length === 1) {
    processNextRequest();
  }
});
