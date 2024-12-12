import Benchmark from 'benchmark'
import { D2, MultiSet, map, join, filter, v } from 'd2ts'

// Sample data generation
const generateData = (size: number) => {
  const users = Array.from({ length: size }, (_, i) => ({
    id: i,
    name: `User ${i}`,
    age: 20 + (i % 40),
  }))

  const posts = Array.from({ length: size * 2 }, (_, i) => ({
    id: i,
    userId: i % size,
    title: `Post ${i}`,
  }))

  return { users, posts }
}

// Test data
const { users, posts } = generateData(1000)

// Convert arrays to MultiSets with key-value pairs
const usersSet = new MultiSet(
  users.map((user) => [[user.id, user] as [number, (typeof users)[0]], 1]),
)
const postsSet = new MultiSet(
  posts.map((post) => [[post.userId, post] as [number, (typeof posts)[0]], 1]),
)

// Benchmark suite for joins
const joinSuite = new Benchmark.Suite('Joins')

// Naive implementation
const naiveJoin = () => {
  return users.flatMap((user) =>
    posts
      .filter((post) => post.userId === user.id)
      .map((post) => ({ userName: user.name, postTitle: post.title })),
  )
}

// D2TS implementation
const joinWithD2TS = () => {
  const graph = new D2({ initialFrontier: v([0]) })
  const usersStream = graph.newInput<[number, (typeof users)[0]]>()
  const postsStream = graph.newInput<[number, (typeof posts)[0]]>()

  // Create join operation using pipe style
  const joined = usersStream.pipe(
    join(postsStream),
    map(
      ([_key, [user, post]]: [
        number,
        [(typeof users)[0], (typeof posts)[0]],
      ]) => ({
        userName: user.name,
        postTitle: post.title,
      }),
    ),
  )

  graph.finalize()

  // Send data to the streams
  usersStream.sendData(v([1]), usersSet)
  postsStream.sendData(v([1]), postsSet)
  graph.step()
  return joined
}

// Add tests
joinSuite
  .add('Naive Join', () => {
    naiveJoin()
  })
  .add('D2TS Join', () => {
    joinWithD2TS()
  })
  .on('cycle', (event: Benchmark.Event) => {
    console.log(String(event.target))
  })
  .on('complete', function (this: Benchmark.Suite) {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })
  .run()

// Benchmark suite for filtering
const filterSuite = new Benchmark.Suite('Filtering')

// Naive implementation
const naiveFilter = () => {
  return users.filter((user) => user.age > 30)
}

// D2TS implementation
const filterWithD2TS = () => {
  const graph = new D2({ initialFrontier: v([0]) })
  const stream = graph.newInput<[number, (typeof users)[0]]>()

  // Create filter operation using pipe style
  const filtered = stream.pipe(
    filter(([_key, user]: [number, (typeof users)[0]]) => user.age > 30),
  )

  graph.finalize()

  // Send data to the stream
  stream.sendData(v([1]), usersSet)
  graph.step()

  return filtered
}

filterSuite
  .add('Naive Filter', () => {
    naiveFilter()
  })
  .add('D2TS Filter', () => {
    filterWithD2TS()
  })
  .on('cycle', (event: Benchmark.Event) => {
    console.log(String(event.target))
  })
  .on('complete', function (this: Benchmark.Suite) {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })
  .run()

// Benchmark suite for mapping
const mapSuite = new Benchmark.Suite('Mapping')

// Naive implementation
const naiveMap = () => {
  const mapped = users.map((user) => ({ name: user.name.toUpperCase() }))
  // console.log(mapped.length)
  // console.log(mapped[0])
  return mapped
}

// D2TS implementation
const mapWithD2TS = () => {
  const graph = new D2({ initialFrontier: v([0]) })
  const stream = graph.newInput<[number, (typeof users)[0]]>()

  const output = stream.pipe(
    map(([id, user]: [number, (typeof users)[0]]) => [
      id,
      { name: user.name.toUpperCase() },
    ]),
  )

  graph.finalize()

  // Send data to the stream
  stream.sendData(v([1]), usersSet)
  graph.step()

  return output
}

mapWithD2TS()

mapSuite
  .add('Naive Map', () => {
    naiveMap()
  })
  .add('D2TS Map', () => {
    mapWithD2TS()
  })
  .on('cycle', (event: Benchmark.Event) => {
    console.log(String(event.target))
  })
  .on('complete', function (this: Benchmark.Suite) {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })
  .run()
