interface SubFunction {
  name: string;
  source: string;
}

export interface SerializeFunction {
  __isFunction: true;
  source: string;
  args: string[];
  values: any[];
  functions: SubFunction[];
}

export type FunctionEnv = { [key: string]: any };

export function serialize(f: Function, env?: FunctionEnv): SerializeFunction {
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

  return {
    __isFunction: true,
    source: f.toString(),
    args,
    values,
    functions,
  };
}

export function deserialize(f: SerializeFunction): Function {
  return new Function(
    ...f.args,
    f.functions
      .map(v => `var ${v.name} = (function(){return ${v.source};})();\n`)
      .join('') +
      'return ' +
      f.source,
  )(...f.values.map(v => (v.__isFunction ? deserialize(v) : v)));
}
