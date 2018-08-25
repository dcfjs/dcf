import { LocalClient } from '../client/LocalClient';
import { Context } from '../client/Context';
import { serialize } from '../common/SerializeFunction';

function isPrime(v: number) {
  const max = Math.sqrt(v) + 0.5;
  for (let i = 2; i < max; i++) {
    if (v % i === 0) {
      return false;
    }
  }
  return true;
}

// Every calculate API is async so we need async/await:
async function main() {
  // Create a local instance of dcf:
  const client = new LocalClient();
  // Wait creation process completed:
  await client.init();
  // Create API context:
  const dcc = new Context(client);

  console.log('Parallelize:');
  let start = Date.now();

  const numbers = dcc.range(2, 1e7);

  console.log(await numbers.filter(isPrime).count());

  console.log(`cost ${Date.now() - start} ms`);

  await numbers.filter(isPrime).saveAsTextFile('./prime');

  console.log('Normal:');
  start = Date.now();
  let count = 0;
  let arr = [];
  for (let v = 2; v < 1e7; v++) {
    arr.push(v);
  }
  console.log(arr.filter(isPrime).length);
  console.log(`cost ${Date.now() - start} ms`);

  // console.log(await dcc.range(0, 20).collect());

  // Shutdown
  client.dispose();
}
main().catch(e => console.error(e.stack));
