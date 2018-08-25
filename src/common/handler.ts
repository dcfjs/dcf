import { Request } from '../client';

export type RequestHandler = (payload: any, context: any) => any | Promise<any>;

const registry: { [key: string]: RequestHandler } = {};

export function registerHandler<T>(key: string, handler: RequestHandler) {
  if (registry[key]) {
    throw new Error(`Duplicated handler registered for ${key}`);
  }
  registry[key] = handler;
}

export function processRequest<T>(
  req: Request<T>,
  context?: any,
): Promise<any> {
  const handler = registry[req.type];
  if (!handler) {
    throw new Error(`No registered handler for ${req.type}`);
  }
  return Promise.resolve(handler(req.payload, context));
}
