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

  // Create a new rdd.
  const content = [...Array(1000000).keys()];
  const rdd = dcc.parallelize(content);

  // Persist it
  const persistRdd = await rdd
    .filter(v => v % 2 === 0)
    .persist('disk');

  console.log(await persistRdd.count());

  // Do something else
  const rdd2 = await persistRdd
    .map((v: any) => [1, v] as any)
    .reduceByKey((a, b) => ((a as number) + (b as number)));

  console.log(await rdd2.take(1));

  // Manually delete persisted data
  await persistRdd.unpersist();

  // Shutdown
  await client.dispose();
}

main().catch(e => console.error(e.stack));
