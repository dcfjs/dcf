import { LocalClient } from '../client/LocalClient';
import { Context } from '../client/Context';

// Every calculate API is async so we need async/await:
async function main() {
  // Create a local instance of dcf:
  const client = new LocalClient();
  // Wait creation process completed:
  await client.init();
  // Create API context:
  const dcc = new Context(client);

  const rdd = dcc.textFile('./src', { recursive: true }).cache();

  console.log('Total files:');
  console.log(await rdd.getNumPartitions());

  console.log('Sample lines:');
  console.log((await rdd.take(10)).join('\n'));

  console.log('Total lines:');
  console.log(await rdd.count());

  console.log('Empty lines:');
  console.log(await rdd.filter(v => v.length === 0).count());

  console.log('Comment lines(only start with //)');
  console.log(await rdd.filter(v => /^\s+\/\//.test(v)).count());

  await rdd.release();
  // Shutdown
  client.dispose();
}
main().catch(e => console.error(e.stack));
