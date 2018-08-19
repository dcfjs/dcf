import { LocalClient } from '../client/LocalClient';
import { Context } from '../client/Context';
import { serialize } from '../common/SerializeFunction';

// Every calculate API is async so we need async/await:
async function main() {
  // Create a local instance of dcf:
  const client = new LocalClient();
  // Wait creation process completed:
  await client.init();
  // Create API context:
  const dcc = new Context(client);

  // Create a new rdd.
  const rdd = dcc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

  console.log(await rdd.repartition(2).collect());

  // Shutdown
  client.dispose();
}
main();
