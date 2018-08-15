import { createContext, runInContext, Context } from 'vm';
import * as readline from 'readline';

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});
rl.setPrompt('> ');

function initContext(): Context {
  return {
    VERSION: require('../../package.json').version,
    exit,
  };
}

function exit() {
  setImmediate(() => {
    rl.pause();
  });
  console.log('Bye.');
}

async function runInContextAsync(line: string, context: Context) {
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

function main() {
  const context = initContext();
  createContext(context);
  rl.prompt();
  rl.on('line', line => {
    runInContextAsync(line, context);
  });
  rl.on('SIGINT', exit);
}
main();
process.on('exit', () => rl.close());
