# Distributed Computing Framework for Node.js

> Early development stage: this project was still under early development, many necessery feature was not done yet, use it on your own risk.

[Document](https://dcf.gitbook.io/dcf/)

[API Reference](https://dcf.gitbook.io/dcf/api/)

A node.js version of [Spark](https://spark.apache.org/), without hadoop or jvm.

You should read [tutorial](src/samples/tutorial-0.ts) first, then you can learn Spark but use this project instead.

## Async API & deferred API

Any api that requires a RDD and generate a result is async, like `count`, `take`, `max` ...
Any api that creates a RDD is deferred API, which is not async, so you can chain them like this:

```js
await dcc
  .parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
  .map(v => v + 1)
  .filter(v => v % 2 === 0)
  .take(10); // take is not deferred api but async
```

## Milestones

#### 0.1.x: Basic

- [x] local master.
- [x] rdd & partition creation & release.
- [x] map & reduce
- [x] repartition & reduceByKey
- [x] disk storage partitions
- [x] cache
- [x] file loader & saver
- [x] export module to npm
- [x] decompresser & compresser
- [x] use debug module for information/error
- [x] provide a progress bar.
- [ ] sample
- [x] sort
- [ ] storage MEMORY_OR_DISK, and use it in sort
- [ ] storage MEMORY_SER，storage in memory but off v8 heap.
- [ ] config default partition count.

#### 0.2.x: Remote mode

- [ ] distributed master
- [ ] runtime sandbox
- [ ] plugin system
- [ ] remote dependency management
- [ ] aliyun oss loader
- [ ] hdfs loader

## How to use

#### Install from npm(shell only)

```
npm install -g dcf
#or
yarn global add dcf
```

Then you can use command: `dcf-shell`

#### Install from npm(as dependency)

```
npm install --save dcf
#or
yarn add dcf
```

Then you can use dcf with javascript or typescript.

#### Run samples & cli

download this repo, install dependencies

```bash
npm install
# or
yarn
```

Run samples:

```bash
npm run ts-node src/samples/tutorial-0.ts
npm run ts-node src/samples/repartition.ts
```

Run interactive cli:

```bash
npm start
```
