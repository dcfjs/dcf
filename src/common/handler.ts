import { Response, Request } from '../client';

export type RequestHandler = (payload: any) => Response | Promise<Response>;

const registry: { [key: string]: RequestHandler } = {};

export function registerHandler(key: string, handler: RequestHandler) {
  registry[key] = handler;
}

export function processRequest(req: Request): Promise<Response> {
  const handler = registry[req.type];
  if (!handler) {
    return Promise.resolve({
      ok: false,
    } as Response);
  }
  return Promise.resolve(handler(req.payload));
}
