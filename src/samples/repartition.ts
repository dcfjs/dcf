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

  console.log(await rdd.coalesce(2).collect());

  const rdd1 = dcc.parallelize<[string, number]>([
    ['a', 1],
    ['b', 2],
    ['a', 3],
    ['b', 4],
    ['a', 5],
    ['b', 6],
    ['a', 7],
    ['b', 8],
  ]);

  console.log(await rdd1.reduceByKey((a, b) => a + b).take(100));

  // Shutdown
  client.dispose();
}
main().catch(e => console.error(e.stack));
