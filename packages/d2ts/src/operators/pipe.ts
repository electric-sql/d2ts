import { IStreamBuilder, PipedOperator } from '../types.js'

// Don't judge, this is the only way to type this function.
// rxjs has very similar code to type its pipe function
// https://github.com/ReactiveX/rxjs/blob/master/packages/rxjs/src/internal/util/pipe.ts
// We go to 20 operators deep, because surly that's enough for anyone...
// A user can always split the pipe into multiple pipes to get around this.
export function pipe<T, O>(o1: PipedOperator<T, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, O>): PipedOperator<T, O>
// prettier-ignore
export function pipe<T, T2, T3, O>(o1: PipedOperator<T, T2>, o2: PipedOperator<T2, T3>, o3: PipedOperator<T3, O>): PipedOperator<T, O>
// ... (remaining overloads)

/**
 * Creates a new stream by piping the input stream through a series of operators
 */
export function pipe<T>(...operators: PipedOperator<any, any>[]) {
  return (stream: IStreamBuilder<T>): IStreamBuilder<T> => {
    return stream.pipe(...operators)
  }
}
