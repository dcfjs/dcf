import { LocalClient } from '../client/LocalClient';
import { createContext, runInContext, Context as ScriptContext } from 'vm';
import * as readline from 'readline';
import { Context } from '../client/Context';

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});
rl.setPrompt('> ');

const client = new LocalClient();

async function initContext(): Promise<ScriptContext> {
  await client.init();
  return {
    VERSION: require('../../package.json').version,
    exit,
    dcc: new Context(client),
  };
}

function exit() {
  setImmediate(() => {
    rl.pause();
    client.dispose();
  });
  console.log('Bye.');
}

async function runInContextAsync(line: string, context: ScriptContext) {
  try {
    rl.pause();
    const result = await runInContext(line, context);
    if (result !== undefined) {
      console.log(result);
    }
  } catch (e) {
    console.error(e);
  } finally {
    rl.prompt();
  }
}

async function main() {
  const context = await initContext();
  createContext(context);
  rl.prompt();
  rl.on('line', line => {
    runInContextAsync(line, context);
  });
  rl.on('SIGINT', exit);
}
main();
process.on('exit', () => rl.close());
