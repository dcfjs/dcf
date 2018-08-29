export interface Client {
  request<T>(m: Request<T>): Promise<any>;
  dispose(): void | Promise<void>;

  workerCount(): number;
}

export interface Request<T> {
  type: string;
  payload?: T;
}

export interface ResponseMessage<T> {
  type: 'resp';
  ok: boolean;
  message?: string;
  payload?: T;
}

export interface DebugMessage {
  type: 'debug';
  msg: string;
  args: any[];
}

export interface ProgressMessage {
  type: 'progress';
  tick: number;
}

export interface TaskMessage {
  type: 'task';
  partitions: number;
  taskIndex: number;
  tasks: number;
}

export type Response =
  | ResponseMessage<any>
  | DebugMessage
  | ProgressMessage
  | TaskMessage;
