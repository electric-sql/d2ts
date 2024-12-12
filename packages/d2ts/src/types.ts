import type { Version, Antichain } from './order'
import type { MultiSet, MultiSetArray } from './multiset'
import type { DifferenceStreamWriter, DifferenceStreamReader } from './graph'

export type KeyValue<K, V> = [K, V]

export const MessageType = {
  DATA: 1,
  FRONTIER: 2,
} as const

export type MessageType = (typeof MessageType)[keyof typeof MessageType]

export type Message<T> =
  | {
      type: typeof MessageType.DATA
      data: DataMessage<T>
    }
  | {
      type: typeof MessageType.FRONTIER
      data: FrontierMessage
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

export interface IDifferenceStreamReader<T> {
  drain(): Message<T>[]
  isEmpty(): boolean
  probeFrontierLessThan(frontier: Antichain): boolean
}

export interface IDifferenceStreamWriter<T> {
  sendData(
    version: Version | number | number[],
    collection: MultiSet<T> | MultiSetArray<T>,
  ): void
  sendFrontier(frontier: Antichain | Version | number | number[]): void
  newReader(): IDifferenceStreamReader<T>
}

export interface ID2 {
  getNextOperatorId(): number
  newInput<T>(): IStreamBuilder<T>
  addOperator(operator: IOperator<any>): void
  addStream(stream: DifferenceStreamReader<any>): void
  frontier(): Antichain
  pushFrontier(newFrontier: Antichain): void
  popFrontier(): void
  finalize(): void
  step(): void
}

export interface IStreamBuilder<T> {
  writer: DifferenceStreamWriter<T>
  connectReader(): DifferenceStreamReader<T>
  graph: ID2
  pipe<O>(o1: PipedOperator<T, O>): IStreamBuilder<O>
}

export type PipedOperator<I, O> = (stream: IStreamBuilder<I>) => IStreamBuilder<O>
