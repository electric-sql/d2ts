import { Version, Antichain } from './order'
import { MultiSetArray } from './multiset'

export enum MessageType {
  DATA = 1,
  FRONTIER = 2,
}

export type Message<T> = {
  type: MessageType
  data: DataMessage<T> | FrontierMessage
}

export type DataMessage<T> = {
  version: Version
  collection: MultiSetArray<T>
}

export type FrontierMessage = Version | Antichain

export interface IOperator<_T> {
  run(): void
  hasPendingWork(): boolean
  frontiers(): [Version[], Version]
}
