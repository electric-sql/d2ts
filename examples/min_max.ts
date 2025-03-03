import { D2, reduce, output } from '@electric-sql/d2ts'
import { MultiSet } from '@electric-sql/d2ts'
import { Antichain, v } from '@electric-sql/d2ts'
import { MessageType, DataMessage } from '@electric-sql/d2ts'

// This example shows how to use the reduce operator to calculate the min and max values 
// for each id in a stream of records. It also demonstrates how removing a record
// (using negative multiplicity) updates the output.

// Create a new D2 graph with an initial frontier of [0, 0]
const graph = new D2({ initialFrontier: v([0, 0]) })

// Create an input stream with records having id and value
type Record = { id: string, value: number }
const input = graph.newInput<[string, Record]>()

// Process the input stream to calculate min and max values for each id
input.pipe(
  // The reduce operator processes all values for each key
  reduce<string, Record, { min: number, max: number }, [string, Record]>((values) => {
    // Skip processing if there are no values after accounting for multiplicities
    const totalMultiplicity = values.reduce((sum, [_, mult]) => sum + mult, 0)
    if (totalMultiplicity === 0) return []

    // Print current values and their multiplicities for debugging
    console.log('\nCurrent values for this key:')
    // Group by value to show clearer multiplicities
    const valueMap = new Map<number, number>()
    for (const [record, multiplicity] of values) {
      const currValue = valueMap.get(record.value) || 0
      valueMap.set(record.value, currValue + multiplicity)
    }
    
    console.log('Grouped by value:')
    for (const [value, multiplicity] of valueMap.entries()) {
      console.log(`Value: ${value}, Total Multiplicity: ${multiplicity}`)
    }
    
    console.log('\nRaw values:')
    for (const [record, multiplicity] of values) {
      console.log(`Value: ${record.value}, Multiplicity: ${multiplicity}`)
    }

    // Find min and max values
    let min = Infinity
    let max = -Infinity
    
    for (const [value, multiplicity] of valueMap.entries()) {
      // Only consider values that are present (positive multiplicity after all operations)
      if (multiplicity > 0) {
        min = Math.min(min, value)
        max = Math.max(max, value)
      }
    }

    console.log(`\nCalculated: Min = ${min}, Max = ${max}`)

    // Return the result with multiplicity 1
    return [[{ min, max }, 1]]
  }),
  
  // Output the results
  output((message) => {
    if (message.type === MessageType.DATA) {
      console.log('\nResults:')
      for (const [[id, result], multiplicity] of (message.data as DataMessage<[string, { min: number, max: number }]>).collection.getInner()) {
        console.log(`ID: ${id}, Min: ${result.min}, Max: ${result.max} (multiplicity: ${multiplicity})`)
      }
      console.log('-'.repeat(50))
    }
  })
)

// Finalize the graph
graph.finalize()

// Let's focus on a clear example with duplicate maximum values
console.log('\n=== DEMONSTRATING DUPLICATE MAX VALUES BEHAVIOR ===')

// Step 1: Add initial records for user4
console.log('\n=== Step 1: Adding initial records with values 10 and 20 ===')
input.sendData(
  v([1, 0]),
  new MultiSet([
    [['user4', { id: 'user4', value: 10 }], 1],
    [['user4', { id: 'user4', value: 20 }], 1],
  ])
)
input.sendFrontier(new Antichain([v([2, 0])]))
graph.run()

// Step 2: Add duplicate records with the same maximum value
console.log('\n=== Step 2: Adding TWO DUPLICATE records with max value 30 ===')
input.sendData(
  v([3, 0]),
  new MultiSet([
    [['user4', { id: 'user4', value: 30 }], 1], // First copy of max value
    [['user4', { id: 'user4', value: 30 }], 1], // Second copy of max value (duplicate)
  ])
)
input.sendFrontier(new Antichain([v([4, 0])]))
graph.run()

// Step 3: Remove one copy of the maximum value
console.log('\n=== Step 3: Removing ONE copy of max value 30 (one still remains) ===')
input.sendData(
  v([5, 0]),
  new MultiSet([
    [['user4', { id: 'user4', value: 30 }], -1], // Remove one copy of max value
  ])
)
input.sendFrontier(new Antichain([v([6, 0])]))
graph.run()

// Step 4: Remove the second copy of the maximum value
console.log('\n=== Step 4: Removing SECOND copy of max value 30 (none remain) ===')
input.sendData(
  v([7, 0]),
  new MultiSet([
    [['user4', { id: 'user4', value: 30 }], -1], // Remove second copy of max value
  ])
)
input.sendFrontier(new Antichain([v([8, 0])]))
graph.run()

console.log('\nExample completed!') 