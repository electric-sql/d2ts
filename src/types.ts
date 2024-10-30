import { Version, Antichain } from './order';
import { CollectionArray } from './base-types';

export enum MessageType {
  DATA = 1,
  FRONTIER = 2
}

export type Message<T> = {
  type: MessageType;
  data: DataMessage<T> | FrontierMessage;
};

export type DataMessage<T> = {
  version: Version;
  collection: CollectionArray<T>;
};

export type FrontierMessage = Version | Antichain;

export interface IOperator<_T> {
  run(): void;
  hasPendingWork(): boolean;
  frontiers(): [Version[], Version];
} 