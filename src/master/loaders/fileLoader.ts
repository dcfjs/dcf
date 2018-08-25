import { parse as parseUrl, resolve } from 'url';
import * as os from 'os';
// import { promises as fs } from 'fs';
const path = require('path');
const fs = require('fs');
import {
  SerializeFunction,
  serialize,
  requireModule,
} from '../../common/SerializeFunction';

const fsPromise = fs.promises;

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
  const stat = await fsPromise.lstat(finalPath);
  if (!stat.isDirectory()) {
    out.push(thisPath);
    return;
  }
  if (!!thisPath && !recursive) {
    // ignore subdirectory for non-recursive mode.
    return;
  }
  const names = await fsPromise.readdir(finalPath);
  for (const name of names) {
    if (name === '.writing') {
      throw new Error(
        "Last write progress is not completed, mabye there's data loss",
      );
    }
    if (name[0] !== '.') {
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
): SerializeFunction<(filename: string) => Buffer | Promise<Buffer>> {
  const basePath = solvePath(baseUrl);

  function loader(filename: string) {
    return fs.promises.readFile(path.resolve(basePath, filename));
  }

  return serialize(loader, {
    basePath,
    fs: requireModule('fs'),
    path: requireModule('path'),
  });
}

async function rmdirs(basePath: string) {
  const stat = await fsPromise.lstat(basePath);
  if (stat.isDirectory()) {
    const contents = await fsPromise.readdir(basePath);
    for (const file of contents) {
      await rmdirs(path.resolve(basePath, file));
    }
  } else {
    await fsPromise.unlink(basePath);
  }
}

export async function initSaveProgress(
  baseUrl: string,
  overwrite: boolean = false,
): Promise<void> {
  const basePath = solvePath(baseUrl);

  try {
    const stat = await fsPromise.lstat(basePath);
    if (!overwrite) {
      throw new Error(
        `${basePath} is already exists, consider use overwrite=true?`,
      );
    }
    await rmdirs(basePath);
  } catch (e) {
    if (e.code === 'ENOENT') {
      await fsPromise.mkdir(basePath);
    } else {
      throw e;
    }
  }

  // create .writing file
  await fsPromise.writeFile(
    path.resolve(basePath, '.writing'),
    Buffer.alloc(0),
  );
}

export function createDataSaver(
  baseUrl: string,
): SerializeFunction<
  (filename: string, buffer: Buffer) => void | Promise<void>
> {
  const basePath = solvePath(baseUrl);
  function saver(filename: string, buffer: Buffer): Promise<void> {
    return fs.promises.writeFile(path.resolve(basePath, filename), buffer);
  }
  return serialize(saver, {
    basePath,
    fs: requireModule('fs'),
    path: requireModule('path'),
  });
}

export async function markSaveSuccess(baseUrl: string): Promise<void> {
  // rename .writing to .success
  const basePath = solvePath(baseUrl);
  await fsPromise.rename(
    path.resolve(basePath, '.writing'),
    path.resolve(basePath, '.success'),
  );
}
