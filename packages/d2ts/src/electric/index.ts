import { D2, RootStreamBuilder } from '../d2.js'
import { MultiSetArray } from '../multiset.js'
import { type Version, type Antichain } from '../order.js'
import {
  type Row,
  type ShapeStreamInterface,
  isChangeMessage,
  isControlMessage,
} from '@electric-sql/client'

/*
Electric uses Postgres LSNs to track progress, each message is annotated with an LSN.
We need to keep track of these and use them to send as the version for each message to 
the D2 input stream.
D2 also requires a a frontier message for be sent, this is the lower bound for all 
future messages.
To do this we:
- Keep track of the last LSN we've seen
- Send the LSN as the version for each message
- When we receive an `up-to-date` message, we send the last LSN+1 as the frontier. The 
  addition of 1 is to account for the fact that the last LSN is the version of the last 
  message, and we need to send the version of the next message as the frontier.
*/

export interface ElectricStreamToD2InputOptions<T extends Row<unknown> = Row> {
  /** D2 Graph to send messages to */
  graph: D2
  /** The Electric ShapeStream to consume */
  stream: ShapeStreamInterface<T>
  /** The D2 input stream to send messages to */
  input: RootStreamBuilder<[key: string, T]>
  /** Optional function to convert LSN to version number/Version */
  lsnToVersion?: (lsn: number) => number | Version
  /** Optional function to convert LSN to frontier number/Antichain */
  lsnToFrontier?: (lsn: number) => number | Antichain
  /** Initial LSN */
  initialLsn?: number
  /** When to run the graph */
  runOn?: 'up-to-date' | 'lsn-advance' | false
  /** Whether to log debug messages */
  debug?: boolean | typeof console.log
}

/**
 * Connects an Electric ShapeStream to a D2 input stream
 * IMPORTANT: Requires the ShapeStream to be configured with `replica: 'full'`
 * @param options Configuration options
 * @param options.stream The Electric ShapeStream to consume
 * @param options.input The D2 input stream to send messages to
 * @param options.lsnToVersion Optional function to convert LSN to version number/Version
 * @param options.lsnToFrontier Optional function to convert LSN to frontier number/Antichain
 * @returns The input stream for chaining
 */
export function electricStreamToD2Input<T extends Row<unknown> = Row>({
  graph,
  stream,
  input,
  lsnToVersion = (lsn: number) => lsn,
  lsnToFrontier = (lsn: number) => lsn,
  initialLsn = 0,
  runOn = 'up-to-date',
  debug = false,
}: ElectricStreamToD2InputOptions<T>): RootStreamBuilder<[key: string, T]> {
  let lastLsn: number = initialLsn
  let changes: MultiSetArray<[key: string, T]> = []

  const sendChanges = (lsn: number) => {
    const version = lsnToVersion(lsn)
    log?.(`sending ${changes.length} changes at version ${version}`)
    if (changes.length > 0) {
      input.sendData(version, [...changes])
    }
    changes = []
  }

  const sendFrontier = (lsn: number) => {
    const frontier = lsnToFrontier(lsn + 1) // +1 to account for the fact that the last LSN is the version of the last message
    log?.(`sending frontier ${frontier}`)
    input.sendFrontier(frontier)
  }

  const log =
    typeof debug === 'function'
      ? debug
      : debug === true
        ? console.log
        : undefined

  log?.('subscribing to stream')
  stream.subscribe((messages) => {
    log?.(`received ${messages.length} messages`)
    for (const message of messages) {
      if (isControlMessage(message)) {
        log?.(`- control message: ${message.headers.control}`)
        // Handle control message
        if (message.headers.control === 'up-to-date') {
          log?.(`up-to-date ${JSON.stringify(message, null, 2)}`)
          if (changes.length > 0) {
            sendChanges(lastLsn)
          }
          if (typeof message.headers.global_last_seen_lsn !== `number`) {
            throw new Error(`global_last_seen_lsn is not a number`)
          }
          const lsn = message.headers.global_last_seen_lsn
          sendFrontier(lsn)
          if (runOn === 'up-to-date' || runOn === 'lsn-advance') {
            log?.('Running graph on up-to-date')
            graph.run()
          }
        } else if (message.headers.control === 'must-refetch') {
          throw new Error(
            'The server sent a "must-refetch" request, this is incompatible with a D2 pipeline and unresolvable. To handle this you will have to remove all state and start the pipeline again.',
          )
        }
      } else if (isChangeMessage(message)) {
        log?.(`- change message: ${message.headers.operation}`)
        // Handle change message
        if (
          message.headers.lsn !== undefined &&
          typeof message.headers.lsn !== `number`
        ) {
          throw new Error(`lsn is not a number`)
        }
        const lsn = message.headers.lsn ?? initialLsn // The LSN is not present on the initial snapshot
        const last: boolean =
          (message.headers.last as boolean | undefined) ?? false
        switch (message.headers.operation) {
          case 'insert':
            changes.push([[message.key, message.value], 1])
            break
          case 'update':
            // An update is a delete followed by an insert
            if (!message.old_value) {
              throw new Error(
                'old_value is undefined, please ensure you have set replica=complete',
              )
            }
            changes.push([[message.key, message.old_value], -1])
            changes.push([[message.key, message.value], 1])
            break
          case 'delete':
            changes.push([[message.key, message.value], -1])
            break
        }
        if (last) {
          sendChanges(lsn)
          sendFrontier(lsn)
          if (runOn === 'lsn-advance') {
            log?.('Running graph on lsn-advance')
            graph.run()
          }
        }
        if (lsn > lastLsn) {
          lastLsn = lsn
        }
      }
    }
  })

  return input
}

/* 
// Used something like this:

// Create D2 graph
const graph = new D2({ initialFrontier: 0 })

// Create D2 input
const input = graph.newInput<any>()

// Configure the pipeline
input
  .pipe(
    map(([key, data]) => data.value),
    filter(value => value > 10)
  )

// Finalize graph
graph.finalize()

// Create Electric stream (example)
const electricStream = new ShapeStream({
  url: 'http://localhost:3000/v1/shape',
  params: {
    table: 'items',
    replica: 'complete',
  }
})

// Connect Electric stream to D2 input
electricStreamToD2Input({
  stream: electricStream,
  input,
})
*/
