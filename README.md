## Distributed Computing Framework for Node.js

> Early development stage: this project was still under early development, many necessery feature was not done yet, use it on your own risk.

A node.js version of [Spark](https://spark.apache.org/), without hadoop or jvm.

You should read [tutorial](src/samples/tutorial-0.ts) first, then you can learn Spark but use this project instead.

#### Async API & deferred API

Any api that requires a RDD and generate a result is async, like `count`, `take`, `max` ...
Any api that creates a RDD is deferred API, which is not async, so you can chain them like this:

```js
await dcc
  .parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
  .map(v => v + 1)
  .filter(v => v % 2 === 0)
  .take(10); // take is not deferred api but async
```

#### Milestones

- [x] local master.
- [x] rdd & partition creation & release.
- [x] map & reduce
- [ ] repartition & reduceByKey
- [ ] distributed master.
- [ ] disk storage partitions
- [ ] cache
- [ ] use debug module for information/error
- [ ] add configure system.
- [ ] file loader/aliyun oss loader
- [ ] decompresser

## How to use

#### Install from npm

Not available yet.

#### Run samples & cli

download this repo, install dependencies

```bash
npm install
```

Run samples:

```bash
npm run ts-node src/samples/tutorial-0.ts
```

Run interactive cli:

```bash
npm start
```
