import { RDD } from './Context';

export function cogroup<K>(
  rdds: RDD<[K, any]>[],
  numPartitions?: number,
): RDD<[K, any[][]]> {
  const rddCount = rdds.length;

  return rdds[0].context
    .union(
      ...rdds.map((v, i) =>
        v.map(
          ([k, v]) => {
            const ret: any[][] = [];
            for (let j = 0; j < rddCount; j++) {
              ret.push(j === i ? [v] : []);
            }
            return [k, ret] as [K, any[][]];
          },
          { rddCount, i },
        ),
      ),
    )
    .reduceByKey((a: any[][], b: any[][]) => {
      const ret = [];
      for (let i = 0; i < a.length; i++) {
        ret.push(a[i].concat(b[i]));
      }
      return ret;
    });
}
