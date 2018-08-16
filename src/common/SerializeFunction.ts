export interface SerializeFunction {
  source: string;
  args: string[];
  values: any[];
}

export type FunctionEnv = { [key: string]: any };

export function serialize(f: Function, env?: FunctionEnv): SerializeFunction {
  const args: string[] = [];
  const values: any[] = [];

  if (env) {
    for (const key of Object.keys(env)) {
      args.push(key);
      values.push(env[key]);
    }
  }

  return {
    source: f.toString(),
    args,
    values,
  };
}

export function deserialize(f: SerializeFunction): Function {
  return new Function(...f.args, 'return ' + f.source)(...f.values);
}
