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
  const rdd = dcc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).cache();

  console.log(
    await rdd
      .union(rdd)
      .distinct()
      .collect(),
  );

  await rdd.unpersist();
  // Shutdown
  client.dispose();
}
main().catch(e => console.error(e.stack));