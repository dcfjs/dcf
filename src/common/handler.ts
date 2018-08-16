import { Response, Request } from '../client';

export type RequestHandler = (payload: any, context: any) => any | Promise<any>;

const registry: { [key: string]: RequestHandler } = {};

export function registerHandler(key: string, handler: RequestHandler) {
  if (registry[key]) {
    throw new Error(`Duplicated handler registered for ${key}`);
  }
  registry[key] = handler;
}

export function processRequest(req: Request, context?: any): Promise<Response> {
  const handler = registry[req.type];
  if (!handler) {
    throw new Error(`No registered handler for ${req.type}`);
  }
  return Promise.resolve(handler(req.payload, context));
}
