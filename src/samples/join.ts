import { LocalClient } from '../client/LocalClient';
import { Context } from '../client/Context';
import { serialize } from '../common/SerializedFunction';

// Every calculate API is async so we need async/await:
async function main() {
  // Create a local instance of dcf:
  const client = new LocalClient();
  // Wait creation process completed:
  await client.init();
  // Create API context:
  const dcc = new Context(client);

  // Create a new rdd.
  const rdd1 = dcc.parallelize([['a', 1], ['b', 2], ['a', 3], ['d', 4]] as [
    string,
    number
  ][]);
  const rdd2 = dcc.parallelize([['a', 5], ['c', 6], ['a', 7], ['b', 8]] as [
    string,
    number
  ][]);

  console.log(JSON.stringify(await rdd1.cogroup(rdd2).collect()));

  console.log(JSON.stringify(await rdd1.join(rdd2).collect()));
  console.log(JSON.stringify(await rdd1.leftOuterJoin(rdd2).collect()));
  console.log(JSON.stringify(await rdd1.rightOuterJoin(rdd2).collect()));
  console.log(JSON.stringify(await rdd1.fullOuterJoin(rdd2).collect()));

  // Shutdown
  client.dispose();
}
main().catch(e => console.error(e.stack));
