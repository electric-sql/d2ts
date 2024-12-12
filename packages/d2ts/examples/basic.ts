import { D2 } from '../src/index.js'
import { map, filter, debug } from '../src/operators.js'

// Create a new D2 graph with an initial frontier of 0
// This is the lower bound of the version space
const graph = new D2({ initialFrontier: 0 })

// A D2 graph can have multiple inputs, here we are creating a single input
// Inputs are typed so that the types can be inferred through the whole graph
const input = graph.newInput<number>()

// We can pipe operators to the input to create a new output
// Each step of the pipe infers its input and output types
const output = input.pipe(
  map((x) => x + 5),
  filter((x) => x % 2 === 0),
  map((x) => x.toString()),
  map((x) => `*${x}*`),
  debug('output'), // <-- this does a console.log of the output
)

// After constructing the graph, we finalize it
// At this point we can no longer add operators or inputs
graph.finalize()

// We can now send data and frontier updates to the graph
// The graph will process the data and frontier updates in a single step
for (let i = 1; i <= 10; i++) {
  // Sending a frontier is informing the graph what the new lower bound of the version
  // space is *on that input*. Each input essentially can have its own lower bound. 
  // These are then passed through all the operators
  input.sendFrontier(i)

  // Sending data to the graph
  // The first param is the version of the data
  // The second param is a MultiSetArray of *changes in the collection*, where the first 
  // element is the record and the second is the multiplicity. A positive multiplicity 
  // means that the record is added to the collection at that version. A negative 
  // multiplicity means that the record is removed from the collection at that version.
  input.sendData(i, [
    [i + 1, 1],
    [i + 2, 1],
    [i - 1, -1],
  ])

  // The graph will process the data and frontier updates in a single step
  graph.step()
}
