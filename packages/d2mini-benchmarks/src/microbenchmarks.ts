import {
  TopKWithFractionalIndexOperator,
  TopKWithFractionalIndexBTreeOperator,
  TopKWithFractionalIndexOperatorOld,
} from '@electric-sql/d2mini'
import { run, bench, boxplot, summary, k_state, do_not_optimize, lineplot } from 'mitata'

type User = {
  id: number
  name: string
  age: number
}

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

// Generate test data and streams of different sizes
const users = [100, 10_000, 100_000] //, 10_000_000]

type TopK =
  | TopKWithFractionalIndexOperator<number, User>
  | TopKWithFractionalIndexBTreeOperator<number, User>
  | TopKWithFractionalIndexOperatorOld<number, User>

const createTopK = (k: number, TopKClass: typeof TopKWithFractionalIndexOperator | typeof TopKWithFractionalIndexBTreeOperator | typeof TopKWithFractionalIndexOperatorOld) => {
  const compare = (user1: User, user2: User) => user1.age - user2.age
  const topK = new TopKClass<number, User>(1, null as any, null as any, compare, { limit: k })
  return topK
}

const populateTopK = (topK: TopK, users: User[]) => {
  const topKResult: Array<[[number, [User, string]], number]> = []
  for (const user of users) {
    topK.processElement(user.id, user, 1, topKResult)
  }
  return topKResult
}

const k = 10

/**
 * Initial sync for a collection of users.
 * This benchmark varies the size of the initial collection to sync.
 */

lineplot(() => {
  boxplot(() => {
    summary(() => {
      bench(
        'topKWithFractionalIndexOld - initial sync ($collectionSize users)',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          yield {
            [0]() {
              return generateUsers(collectionSize)
            },
            [1]() {
              return createTopK(k, TopKWithFractionalIndexOperatorOld)
            },
            bench(users: User[], topK: TopK) {
              return do_not_optimize(populateTopK(topK, users))
            },
          }
        },
      ).args({
        collectionSize: users,
      })

      bench(
        'topKWithFractionalIndex - initial sync ($collectionSize users)',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          yield {
            [0]() {
              return generateUsers(collectionSize)
            },
            [1]() {
              return createTopK(k, TopKWithFractionalIndexOperator)
            },
            bench(users: User[], topK: TopK) {
              return do_not_optimize(populateTopK(topK, users))
            },
          }
        },
      ).args({
        collectionSize: users,
      }) // inner mode runs gc after warmup and before each (batch-)iteration
      //.gc('inner')

      bench(
        'topKWithFractionalIndexBTree - initial sync ($collectionSize users)',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          yield {
            [0]() {
              return generateUsers(collectionSize)
            },
            [1]() {
              return createTopK(k, TopKWithFractionalIndexBTreeOperator)
            },
            bench(users: User[], topK: TopK) {
              return do_not_optimize(populateTopK(topK, users))
            },
          }
        },
      ).args({
        collectionSize: users,
      }) // inner mode runs gc after warmup and before each (batch-)iteration
      //.gc('inner')
    })
  })
})

/**
 * Inserting 1 user in a collection of users.
 * This benchmark varies the collection size.
 */

lineplot(() => {
  boxplot(() => {
    summary(() => {
      bench(
        'topKWithFractionalIndexOld - $collectionSize users',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          const topK = createTopK(k, TopKWithFractionalIndexOperatorOld)
          const initialUsers = generateUsers(collectionSize)
          populateTopK(topK, initialUsers)
          yield {
            [0]() {
              return generateUsers(1)
            },
            [1]() {
              return topK
            },
            bench(users: User[], topK: TopK) {
              return do_not_optimize(populateTopK(topK, users))
            },
          }
        },
      ).args({
        collectionSize: users,
      })
      
      bench(
        'topKWithFractionalIndex - $collectionSize users',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          const topK = createTopK(k, TopKWithFractionalIndexOperator)
          const initialUsers = generateUsers(collectionSize)
          populateTopK(topK, initialUsers)
          yield {
            [0]() {
              return generateUsers(1)
            },
            [1]() {
              return topK
            },
            bench(users: User[], topK: TopK) {
              return do_not_optimize(populateTopK(topK, users))
            },
          }
        },
      ).args({
        collectionSize: users,
      }) // inner mode runs gc after warmup and before each (batch-)iteration
      //.gc('inner')

      bench(
        'topKWithFractionalIndexBTree - $collectionSize users',
        function* (state: k_state) {
          const collectionSize = state.get('collectionSize') as number
          const topK = createTopK(k, TopKWithFractionalIndexBTreeOperator)
          const initialUsers = generateUsers(collectionSize)
          populateTopK(topK, initialUsers)
          yield {
            [0]() {
              return generateUsers(1)
            },
            [1]() {
              return topK
            },
            bench(users: User[], topK: TopK) {
              return do_not_optimize(populateTopK(topK, users))
            },
          }
        },
      ).args({
        collectionSize: users,
      }) // inner mode runs gc after warmup and before each (batch-)iteration
      //.gc('inner')
    })
  })
})

run().then((res) => {
  console.log(res.benchmarks.length)
})
