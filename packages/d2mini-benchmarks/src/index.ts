import {
  D2,
  MultiSet,
  output,
  topKWithIndex,
  topKWithFractionalIndex,
  topKWithFractionalIndexBTree,
  topKWithFractionalIndexOld,
} from '@electric-sql/d2mini'
import { run, bench, boxplot, summary, k_state, do_not_optimize, lineplot } from 'mitata'

type User = {
  id: number
  name: string
  age: number
}

type TopKOperatorName = 'topKWithIndex' | 'topKWithFractionalIndex' | 'topKWithFractionalIndexBTree' | 'topKWithFractionalIndexOld'
type TopKOperator =
  | typeof topKWithIndex
  | typeof topKWithFractionalIndex
  | typeof topKWithFractionalIndexBTree
  | typeof topKWithFractionalIndexOld

let i = 1

// Sample data generation
const generateUsers = (size: number) => {
  const users = Array.from({ length: size }, (_, _i) => ({
    id: i++,
    name: `User ${i}`,
    age: getRandomInt(1, 100_000_000),
  }))

  return users
}

function getRandomInt(min: number, max: number) {
  min = Math.ceil(min)
  max = Math.floor(max)
  return Math.floor(Math.random() * (max - min + 1)) + min
}

type Stream = ReturnType<typeof createUserStream>

function createUserStream(topK: TopKOperator) {
  const graph = new D2()
  const input = graph.newInput<[number, User]>()
  
  input.pipe(
    topK((user1: User, user2: User) => user1.age - user2.age, { limit: 10 }),
    output((_data) => {
      // do nothing
    }),
  )

  graph.finalize()

  return { graph, input }
}

function populateStream(
  users: User[],
  stream: Stream,
) {
  stream.input.sendData(
    new MultiSet(
      users.map((user) => [[user.id, user], 1]),
    ),
  )
  stream.graph.run()
}

/**************************
 * Initial sync benchmark *
 **************************/

const initialSyncSizes = [100, 10_000, 100_000] //, 500_000]

/*
lineplot(() => {
  boxplot(() => {
    summary(() => {
      bench(
        'topKWithIndex - initial sync ($collectionSize users)',
        function* (state: k_state) {
          yield {
            [0]() {
              return generateUsers(state.get('collectionSize'))
            },
            [1]() {
              return createUserStream(topKWithIndex)
            },
            bench(users: User[], stream: Stream) {
              return do_not_optimize(populateStream(users, stream))
            },
          }
        },
      )
        .args({
          collectionSize: initialSyncSizes,
        })
        // inner mode runs gc after warmup and before each (batch-)iteration
      //.gc('inner')
    
      bench(
        'topKWithFractionalIndexOld - initial sync ($collectionSize users)',
        function* (state: k_state) {
          yield {
            [0]() {
              return generateUsers(state.get('collectionSize'))
            },
            [1]() {
              return createUserStream(topKWithFractionalIndexOld)
            },
            bench(users: User[], stream: Stream) {
              return do_not_optimize(populateStream(users, stream))
            },
          }
        },
      )
        .args({
          collectionSize: initialSyncSizes,
        })
        // inner mode runs gc after warmup and before each (batch-)iteration
        //.gc('inner')
    
      bench(
        'topKWithFractionalIndex - initial sync ($collectionSize users)',
        function* (state: k_state) {
          yield {
            [0]() {
              return generateUsers(state.get('collectionSize'))
            },
            [1]() {
              return createUserStream(topKWithFractionalIndex)
            },
            bench(users: User[], stream: Stream) {
              return do_not_optimize(populateStream(users, stream))
            },
          }
        },
      )
        .args({
          collectionSize: initialSyncSizes,
        })
        // inner mode runs gc after warmup and before each (batch-)iteration
        //.gc('inner')
    
      bench(
        'topKWithFractionalIndexBTree - initial sync ($collectionSize users)',
        function* (state: k_state) {
          yield {
            [0]() {
              return generateUsers(state.get('collectionSize'))
            },
            [1]() {
              return createUserStream(topKWithFractionalIndexBTree)
            },
            bench(users: User[], stream: Stream) {
              return do_not_optimize(populateStream(users, stream))
            },
          }
        },
      )
        .args({
          collectionSize: initialSyncSizes,
        })
        // inner mode runs gc after warmup and before each (batch-)iteration
        //.gc('inner')
    })
  })
})
*/

/***************************************************************
 * Varying collection size with 1 incremental update benchmark *
 ****************************************************************/

function addUsers(users: User[], stream: Stream) {
  stream.input.sendData(new MultiSet(users.map((user) => [[user.id, user], 1])))
  stream.graph.run()
}

// Generate test data and streams of different sizes
const users = [100, 10_000, 100_000] //, 1_000_000] //, 10_000_000]

const createStream = (topK: TopKOperator, size: number) => {
  const stream = createUserStream(topK)
  populateStream(generateUsers(size), stream)
  return stream
}

lineplot(() => {
  boxplot(() => {
    summary(() => {
      bench('topKWithIndex - $collectionSize users', function* (state: k_state) {
        const collectionSize = state.get('collectionSize') as number
        const stream = createStream(topKWithIndex, collectionSize)
        yield {
          [0]() {
            return generateUsers(1)
          },
          [1]() {
            return stream
          },
          bench(usersToAdd: User[], stream: Stream) {
            return do_not_optimize(addUsers(usersToAdd, stream))
          }
        }
      })
        .args({
          collectionSize: users,
        }) // inner mode runs gc after warmup and before each (batch-)iteration
      //.gc('inner')
      
      bench(
        'topKWithFractionalIndexOld - $collectionSize users',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          const stream = createStream(
            topKWithFractionalIndexOld,
            collectionSize,
          )
          yield {
            [0]() {
              return generateUsers(1)
            },
            [1]() {
              return stream
            },
            bench(usersToAdd: User[], stream: Stream) {
              return do_not_optimize(addUsers(usersToAdd, stream))
            },
          }
        },
      )
        .args({
          collectionSize: users,
        })
        //.gc('inner')

      bench(
        'topKWithFractionalIndex - $collectionSize users',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          const stream = createStream(topKWithFractionalIndex, collectionSize)
          yield {
            [0]() {
              return generateUsers(1)
            },
            [1]() {
              return stream
            },
            bench(usersToAdd: User[], stream: Stream) {
              return do_not_optimize(addUsers(usersToAdd, stream))
            },
          }
        },
      )
        .args({
          collectionSize: users,
        }) // inner mode runs gc after warmup and before each (batch-)iteration
        //.gc('inner')

      bench(
        'topKWithFractionalIndexBTree - $collectionSize users',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          const stream = createStream(topKWithFractionalIndexBTree, collectionSize)
          yield {
            [0]() {
              return generateUsers(1)
            },
            [1]() {
              return stream
            },
            bench(usersToAdd: User[], stream: Stream) {
              return do_not_optimize(addUsers(usersToAdd, stream))
            },
          }
        },
      )
        .args({
          collectionSize: users,
        }) // inner mode runs gc after warmup and before each (batch-)iteration
        //.gc('inner')

    })
  })
})

/********************************************************
 * Varying updates with fixed collection size benchmark *
 ********************************************************/

const usersToAdd = [1, 100, 1000, 10_000, 100_000] //, 500_000, 750_000] //[10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 100_000]
const collectionSize = [10_000]

lineplot(() => {
  boxplot(() => {
    summary(() => {
      bench(
        'topKWithIndex - $collectionSize users, $usersToAdd updates',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          yield {
            [0]() {
              return generateUsers(state.get('usersToAdd') as number)
            },
            [1]() {
              return createStream(topKWithIndex, collectionSize)
            },
            bench(usersToAdd: User[], stream: Stream) {
              return do_not_optimize(addUsers(usersToAdd, stream))
            },
          }
        },
      )
        .args({
          collectionSize,
          usersToAdd,
        }) // inner mode runs gc after warmup and before each (batch-)iteration
      //.gc('inner')

      bench(
        'topKWithFractionalIndexOld - $collectionSize users, $usersToAdd updates',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          yield {
            [0]() {
              return generateUsers(state.get('usersToAdd') as number)
            },
            [1]() {
              return createStream(topKWithFractionalIndexOld, collectionSize)
            },
            bench(usersToAdd: User[], stream: Stream) {
              return do_not_optimize(addUsers(usersToAdd, stream))
            },
          }
        },
      )
        .args({
          collectionSize,
          usersToAdd,
        }) // inner mode runs gc after warmup and before each (batch-)iteration
      //.gc('inner')
      
      bench(
        'topKWithFractionalIndex - $collectionSize users, $usersToAdd updates',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          yield {
            [0]() {
              return generateUsers(state.get('usersToAdd') as number)
            },
            [1]() {
              return createStream(topKWithFractionalIndex, collectionSize)
            },
            bench(usersToAdd: User[], stream: Stream) {
              return do_not_optimize(addUsers(usersToAdd, stream))
            },
          }
        },
      )
        .args({
          collectionSize,
          usersToAdd,
        }) // inner mode runs gc after warmup and before each (batch-)iteration
        //.gc('inner')

      
      bench(
        'topKWithFractionalIndexBTree - $collectionSize users, $usersToAdd updates',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          yield {
            [0]() {
              return generateUsers(state.get('usersToAdd') as number)
            },
            [1]() {
              return createStream(topKWithFractionalIndexBTree, collectionSize)
            },
            bench(usersToAdd: User[], stream: Stream) {
              return do_not_optimize(addUsers(usersToAdd, stream))
            },
          }
        },
      )
        .args({
          collectionSize,
          usersToAdd,
        }) // inner mode runs gc after warmup and before each (batch-)iteration
        //.gc('inner')
      
      
    })
  })
})

run().then((res) => {
  console.log(res.benchmarks.length)
})
