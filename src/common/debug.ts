let debug: (msg: string, ...args: any[]) => void = () => {};

export function setDebugFunc(func: (msg: string, ...args: any[]) => void) {
  debug = func;
}

export default function(msg: string, ...args: any[]) {
  debug(msg, ...args);
}
