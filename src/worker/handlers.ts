import { registerHandler } from '../common/handler';

export const INIT = '@@worker/init';
export const EXIT = '@@worker/exit';

let id = 'NO-ID';

registerHandler(INIT, (_id: string) => {
  id = _id;
  console.log(`Worker ${id} inited.`);

  return {
    ok: true,
  };
});

registerHandler(EXIT, () => {
  console.log(`Worker ${id} exited`);
  process.exit(0);
  return {
    ok: true,
  };
});
