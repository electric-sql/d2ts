import Benchmark from 'benchmark'
import { GraphBuilder, MultiSet, Antichain } from 'd2ts'

// Sample data generation
const generateData = (size: number) => {
  const users = Array.from({ length: size }, (_, i) => ({
    id: i,
    name: `User ${i}`,
    age: 20 + (i % 40)
  }))

  const posts = Array.from({ length: size * 2 }, (_, i) => ({
    id: i,
    userId: i % size,
    title: `Post ${i}`
  }))

  return { users, posts }
}

// Test data
const { users, posts } = generateData(1000)

// Benchmark suite for joins
const joinSuite = new Benchmark.Suite('Joins')

// Naive implementation
const naiveJoin = () => {
  return users.flatMap(user => 
    posts
      .filter(post => post.userId === user.id)
      .map(post => ({ userName: user.name, postTitle: post.title }))
  )
}

// D2TS implementation
const joinWithD2TS = () => {
  const builder = new GraphBuilder(new Antichain([]))
  const [usersStream, usersWriter] = builder.newInput<[number, typeof users[0]]>()
  const [postsStream, postsWriter] = builder.newInput<[number, typeof posts[0]]>()

  // Convert arrays to MultiSets with key-value pairs
  const usersSet = new MultiSet(users.map(user => [[user.id, user] as [number, typeof users[0]], 1]))
  const postsSet = new MultiSet(posts.map(post => [[post.userId, post] as [number, typeof posts[0]], 1]))

  // Send data to the streams
  usersWriter.sendData(0, usersSet)
  postsWriter.sendData(0, postsSet)
  usersWriter.sendFrontier(0)
  postsWriter.sendFrontier(0)

  // Create join operation
  const joined = usersStream.join(postsStream).map(([_, [user, post]]) => ({
    userName: user.name,
    postTitle: post.title
  }))

  const graph = builder.finalize()
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
  .on('complete', function(this: Benchmark.Suite) {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })
  .run()

// Benchmark suite for filtering
const filterSuite = new Benchmark.Suite('Filtering')

// Naive implementation
const naiveFilter = () => {
  return users.filter(user => user.age > 30)
}

// D2TS implementation
const filterWithD2TS = () => {
  const builder = new GraphBuilder(new Antichain([]))
  const [stream, writer] = builder.newInput<[number, typeof users[0]]>()

  // Convert array to MultiSet with key-value pairs
  const usersSet = new MultiSet(users.map(user => [[user.id, user] as [number, typeof users[0]], 1]))

  // Send data to the stream
  writer.sendData(0, usersSet)
  writer.sendFrontier(0)

  // Create filter operation
  const filtered = stream.filter(([_, user]) => user.age > 30)

  const graph = builder.finalize()
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
  .on('complete', function(this: Benchmark.Suite) {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })
  .run()

// Benchmark suite for mapping
const mapSuite = new Benchmark.Suite('Mapping')

// Naive implementation
const naiveMap = () => {
  return users.map(user => ({ name: user.name.toUpperCase() }))
}

// D2TS implementation
const mapWithD2TS = () => {
  const builder = new GraphBuilder(new Antichain([]))
  const [stream, writer] = builder.newInput<[number, typeof users[0]]>()

  // Convert array to MultiSet with key-value pairs
  const usersSet = new MultiSet(users.map(user => [[user.id, user] as [number, typeof users[0]], 1]))

  // Send data to the stream
  writer.sendData(0, usersSet)
  writer.sendFrontier(0)

  // Create map operation
  const mapped = stream.map(([_, user]) => ({ name: user.name.toUpperCase() }))

  const graph = builder.finalize()
  graph.step()
  return mapped
}

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
  .on('complete', function(this: Benchmark.Suite) {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })
  .run() 