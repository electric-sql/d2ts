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
Currently the LSN is a string in the format "LSN_sequence", we need to extract the 
number from this to use as the version for each message. In the future we intend to add
the LSN as a header to each message, so we can remove this logic.
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

function extractLSN(offset: string): number {
  // Extract LSN from format "LSN_sequence"
  const lsn = parseInt(offset.split('_')[0])
  if (isNaN(lsn)) {
    throw new Error(`Invalid LSN format: ${offset}`)
  }
  return lsn
}

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
}: ElectricStreamToD2InputOptions<T>): RootStreamBuilder<[key: string, T]> {
  let lastLsn: number = initialLsn
  let changes: MultiSetArray<[key: string, T]> = []

  const sendChanges = (lsn: number) => {
    const version = lsnToVersion(lsn)
    if (changes.length > 0) {
      input.sendData(version, [...changes])
    }
    changes = []
  }

  stream.subscribe((messages) => {

    for (const message of messages) {
      if (isControlMessage(message)) {
        // Handle control message
        if (message.headers.control === 'up-to-date') {
          sendChanges(lastLsn)
          const frontier = lsnToFrontier(lastLsn + 1) // +1 to account for the fact that the last LSN is the version of the last message
          input.sendFrontier(frontier)
          if (runOn === 'up-to-date' || runOn === 'lsn-advance') {
            graph.run()
          }
        }
      } else if (isChangeMessage(message)) {
        // Handle change message
        const lsn = extractLSN(message.offset)
        switch (message.headers.operation) {
          case 'insert':
            changes.push([[message.key, message.value], 1])
            break
          case 'update':
            // An update is a delete followed by an insert
            changes.push([[message.key, message.value], -1]) // We don't have the old value, TODO: check if this causes issues
            changes.push([[message.key, message.value], 1])
            break
          case 'delete':
            changes.push([[message.key, message.value], -1])
            break
        }
        if (lsn !== lastLsn) {
          sendChanges(lsn)
          if (runOn === 'lsn-advance') {
            graph.run()
          }
        }
        lastLsn = lsn
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
    replica: 'full',
  }
})

// Connect Electric stream to D2 input
electricStreamToD2Input({
  stream: electricStream,
  input,
})
*/
