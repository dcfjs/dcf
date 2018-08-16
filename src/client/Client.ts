export interface Client {
  request(m: Request): Promise<any>;
  dispose(): void | Promise<void>;

  workerCount(): number;
}

export interface Request {
  type: string;
  payload?: any;
}

export interface Response {
  ok: boolean;
  message?: string;
  payload?: any;
}
