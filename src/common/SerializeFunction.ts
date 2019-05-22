import debug from '../common/debug';

interface SubFunction {
  name: string;
  source: string;
}

interface SerializedFunctionStruct {
  __isFunction: true;
  source: string;
  args: string[];
  values: any[];
  functions: SubFunction[];
}

// Make serialized function seems callable, for better usage with upvalue functions.
// But this could cause a error when you call it before serialize & deserialize.
export type SerializedFunction<T extends (...args: any[]) => any> = T &
  SerializedFunctionStruct;

export type FunctionEnv = { [key: string]: any };

export function serialize<T extends (...args: any[]) => any>(
  f: T,
  env?: FunctionEnv,
): SerializedFunction<T> {
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
  } as SerializedFunctionStruct) as SerializedFunction<T>;
}

export function requireModule(module: string) {
  return {
    __isRequire: true,
    module,
  };
}

function wrap<T extends (...args: any[]) => any>(f: T) {
  return function(...args: any[]) {
    try {
      return f(...args);
    } catch (e) {
      console.error(`In function: ${f.toString()}`);
      throw e;
    }
  };
}

let requireWhiteList: { [key: string]: 1 } | undefined;

function safeRequire(m: string): any {
  if (requireWhiteList && !requireWhiteList[m]) {
    throw new Error(
      `Module ${m} is forbidden for required, please contact your administrator.`,
    );
  }
  return require(m);
}

export function deserialize<T extends (...args: any[]) => any>(
  f: SerializedFunction<T>,
): T {
  return new Function(
    'debug',
    'wrap',
    ...f.args,
    f.functions
      .map(v => `var ${v.name} = (function(){return wrap(${v.source});})();\n`)
      .join('') +
      'return wrap(' +
      f.source +
      ')',
  )(
    debug,
    wrap,
    ...f.values.map(v =>
      v && v.__isFunction
        ? deserialize(v)
        : v && v.__isRequire
        ? safeRequire(v.module)
        : v,
    ),
  );
}

export function setRequireWhiteList(whiteList: string[]) {
  requireWhiteList = {};
  for (const mod of whiteList) {
    requireWhiteList[mod] = 1;
  }
}
