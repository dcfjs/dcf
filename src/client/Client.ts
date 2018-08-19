export interface Client {
  request<T>(m: Request<T>): Promise<any>;
  dispose(): void | Promise<void>;

  workerCount(): number;
}

export interface Request<T> {
  type: string;
  payload?: T;
}

export interface Response<T> {
  ok: boolean;
  message?: string;
  payload?: T;
}
