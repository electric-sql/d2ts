import { Version, Antichain } from './order'
import { MultiSet } from './multiset'

export const MessageType = {
  DATA: 1,
  FRONTIER: 2,
} as const

export type MessageType = typeof MessageType[keyof typeof MessageType]

export type Message<T> = {
  type: MessageType
  data: DataMessage<T> | FrontierMessage
}

export type DataMessage<T> = {
  version: Version
  collection: MultiSet<T>
}

export type FrontierMessage = Version | Antichain

export interface IOperator<_T> {
  run(): void
  hasPendingWork(): boolean
  frontiers(): [Antichain[], Antichain]
}
