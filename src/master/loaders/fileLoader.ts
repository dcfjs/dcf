import { parse as parseUrl, resolve } from 'url';
import * as os from 'os';
// import { promises as fs } from 'fs';
const path = require('path');
const fs = require('fs-promise');
import {
  SerializedFunction,
  serialize,
  requireModule,
} from '../../common/SerializeFunction';

export function canHandleUrl(baseUrl: string): boolean {
  const url = parseUrl(baseUrl);
  if (url.protocol && url.protocol !== 'file:') {
    return false;
  }
  return true;
}

function solvePath(baseUrl: string): string {
  const url = parseUrl(baseUrl);
  if (url.protocol) {
    // absolute path
    const ret = url.path || '/';
    if (os.platform() === 'win32') {
      return path.resolve(ret.substr(1));
    }
    return path.resolve(ret);
  } else {
    // maybe relative
    return path.resolve(process.cwd(), baseUrl);
  }
}

async function listFilesInPath(
  basePath: string,
  thisPath: string,
  recursive: boolean,
  out: string[],
) {
  const finalPath = path.resolve(basePath, thisPath);
  const stat = await fs.lstat(finalPath);
  if (!stat.isDirectory()) {
    out.push(thisPath);
    return;
  }
  if (!!thisPath && !recursive) {
    // ignore subdirectory for non-recursive mode.
    return;
  }
  const names = await fs.readdir(finalPath);
  for (const name of names) {
    if (name === '.writing') {
      throw new Error(
        "Last write progress is not completed, mabye there's data loss",
      );
    }
    if (!name.startsWith('.')) {
      await listFilesInPath(
        basePath,
        path.join(thisPath, name),
        recursive,
        out,
      );
    }
  }
}

export async function listFiles(
  baseUrl: string,
  recursive: boolean = false,
): Promise<string[]> {
  const basePath = solvePath(baseUrl);
  const out: string[] = [];

  await listFilesInPath(basePath, '', recursive, out);

  return out;
}

export function createDataLoader(
  baseUrl: string,
): SerializedFunction<(filename: string) => Buffer | Promise<Buffer>> {
  const basePath = solvePath(baseUrl);

  function loader(filename: string) {
    return fs.readFile(path.resolve(basePath, filename));
  }

  return serialize(loader, {
    basePath,
    fs: requireModule('fs-promise'),
    path: requireModule('path'),
  });
}

async function rmdirs(basePath: string) {
  const stat = await fs.lstat(basePath);
  if (stat.isDirectory()) {
    const contents = await fs.readdir(basePath);
    for (const file of contents) {
      await rmdirs(path.resolve(basePath, file));
    }
  } else {
    await fs.unlink(basePath);
  }
}

export async function initSaveProgress(
  baseUrl: string,
  overwrite: boolean = false,
): Promise<void> {
  const basePath = solvePath(baseUrl);

  try {
    const stat = await fs.lstat(basePath);
    if (!overwrite) {
      throw new Error(
        `${basePath} already exists, consider use overwrite=true?`,
      );
    }
    await rmdirs(basePath);
  } catch (e) {
    if (e.code === 'ENOENT') {
      await fs.mkdir(basePath);
    } else {
      throw e;
    }
  }

  // create .writing file
  await fs.writeFile(path.resolve(basePath, '.writing'), Buffer.alloc(0));
}

export function createDataSaver(
  baseUrl: string,
): SerializedFunction<
  (filename: string, buffer: Buffer) => void | Promise<void>
> {
  const basePath = solvePath(baseUrl);
  function saver(filename: string, buffer: Buffer): Promise<void> {
    return fs.writeFile(path.resolve(basePath, filename), buffer);
  }
  return serialize(saver, {
    basePath,
    fs: requireModule('fs-promise'),
    path: requireModule('path'),
  });
}

export async function markSaveSuccess(baseUrl: string): Promise<void> {
  // rename .writing to .success
  const basePath = solvePath(baseUrl);
  await fs.rename(
    path.resolve(basePath, '.writing'),
    path.resolve(basePath, '.success'),
  );
}
