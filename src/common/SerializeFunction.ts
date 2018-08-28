import { SerializeFunction } from './SerializeFunction';
interface SubFunction {
  name: string;
  source: string;
}

interface SerializeFunctionStruct {
  __isFunction: true;
  source: string;
  args: string[];
  values: any[];
  functions: SubFunction[];
}

// Make serialized function seems callable, for better usage with upvalue functions.
// But this could cause a error when you call it before serialize & deserialize.
export type SerializeFunction<T extends (...args: any[]) => any> = T &
  SerializeFunctionStruct;

export type FunctionEnv = { [key: string]: any };

export function serialize<T extends (...args: any[]) => any>(
  f: T,
  env?: FunctionEnv,
): SerializeFunction<T> {
  const args: string[] = [];
  const values: any[] = [];
  const functions: any[] = [];

  if (env) {
    for (const key of Object.keys(env)) {
      if (typeof env[key] === 'function') {
        functions.push({
          name: key,
          source: env[key].toString(),
        });
      } else {
        args.push(key);
        values.push(env[key]);
      }
    }
  }

  return ({
    __isFunction: true,
    source: f.toString(),
    args,
    values,
    functions,
  } as SerializeFunctionStruct) as SerializeFunction<T>;
}

export function requireModule(module: string) {
  return {
    __isRequire: true,
    module,
  };
}

export function deserialize<T extends (...args: any[]) => any>(
  f: SerializeFunction<T>,
): T {
  return new Function(
    ...f.args,
    f.functions
      .map(v => `var ${v.name} = (function(){return ${v.source};})();\n`)
      .join('') +
      'return ' +
      f.source,
  )(
    ...f.values.map(
      v =>
        v && v.__isFunction
          ? deserialize(v)
          : v && v.__isRequire
            ? require(v.module)
            : v,
    ),
  );
}
