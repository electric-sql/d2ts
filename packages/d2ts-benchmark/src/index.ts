import Benchmark from 'benchmark'
import { D2, MultiSet, map, join, filter, v } from 'd2ts'
import { join as joinSql } from 'd2ts/sqlite'
import { BetterSQLite3Wrapper } from 'd2ts/sqlite'
import Database from 'better-sqlite3'

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

// Test data - generate 1000 items but split into initial and incremental sets
const totalSize = 1000
const initialSize = 900
const { users: allUsers, posts: allPosts } = generateData(totalSize)

const initialUsers = allUsers.slice(0, initialSize)
const incrementalUsers = allUsers.slice(initialSize)
const initialPosts = allPosts.slice(0, initialSize * 2)
const incrementalPosts = allPosts.slice(initialSize * 2)

// Convert initial arrays to MultiSets
const initialUsersSet = new MultiSet(
  initialUsers.map((user) => [[user.id, user] as [number, (typeof allUsers)[0]], 1]),
)
const initialPostsSet = new MultiSet(
  initialPosts.map((post) => [[post.userId, post] as [number, (typeof allPosts)[0]], 1]),
)

// Benchmark suite for joins
const joinSuite = new Benchmark.Suite('Incremental Joins')

// Naive implementation with incremental updates
const naiveJoin = () => {
  // Initial join with 900 items
  let result = initialUsers.flatMap((user) =>
    initialPosts
      .filter((post) => post.userId === user.id)
      .map((post) => ({ userName: user.name, postTitle: post.title })),
  )

  // Add one item at a time and recompute
  let currentUsers = [...initialUsers]
  let currentPosts = [...initialPosts]

  for (let i = 0; i < incrementalUsers.length; i++) {
    currentUsers.push(incrementalUsers[i])
    currentPosts.push(incrementalPosts[i * 2])
    currentPosts.push(incrementalPosts[i * 2 + 1])

    result = currentUsers.flatMap((user) =>
      currentPosts
        .filter((post) => post.userId === user.id)
        .map((post) => ({ userName: user.name, postTitle: post.title })),
    )
  }

  return result
}

// D2TS implementation with incremental updates
const joinWithD2TS = () => {
  const graph = new D2({ initialFrontier: v([0]) })
  const usersStream = graph.newInput<[number, (typeof allUsers)[0]]>()
  const postsStream = graph.newInput<[number, (typeof allPosts)[0]]>()

  const joined = usersStream.pipe(
    join(postsStream),
    map(
      ([_key, [user, post]]: [
        number,
        [(typeof allUsers)[0], (typeof allPosts)[0]],
      ]) => ({
        userName: user.name,
        postTitle: post.title,
      }),
    ),
  )

  graph.finalize()

  // Send initial data
  usersStream.sendData(v([1]), initialUsersSet)
  postsStream.sendData(v([1]), initialPostsSet)
  graph.step()

  // Incrementally add remaining items
  for (let i = 0; i < incrementalUsers.length; i++) {
    const user = incrementalUsers[i]
    const post1 = incrementalPosts[i * 2]
    const post2 = incrementalPosts[i * 2 + 1]

    usersStream.sendData(
      v([i + 2]),
      new MultiSet([[[user.id, user] as [number, (typeof allUsers)[0]], 1]]),
    )
    postsStream.sendData(
      v([i + 2]),
      new MultiSet([
        [[post1.userId, post1] as [number, (typeof allPosts)[0]], 1],
        [[post2.userId, post2] as [number, (typeof allPosts)[0]], 1],
      ]),
    )
    graph.step()
  }

  return joined
}

// SQLite-based D2TS implementation
const joinWithD2TSAndSQLite = () => {
  const sqlite = new Database(':memory:')
  const db = new BetterSQLite3Wrapper(sqlite)
  
  const graph = new D2({ initialFrontier: v([0]) })
  const usersStream = graph.newInput<[number, (typeof allUsers)[0]]>()
  const postsStream = graph.newInput<[number, (typeof allPosts)[0]]>()

  const joined = usersStream.pipe(
    joinSql(postsStream, db),
    map(
      ([_key, [user, post]]: [
        number,
        [(typeof allUsers)[0], (typeof allPosts)[0]],
      ]) => ({
        userName: user.name,
        postTitle: post.title,
      }),
    ),
  )

  graph.finalize()

  // Send initial data
  usersStream.sendData(v([1]), initialUsersSet)
  postsStream.sendData(v([1]), initialPostsSet)
  graph.step()

  // Incrementally add remaining items
  for (let i = 0; i < incrementalUsers.length; i++) {
    const user = incrementalUsers[i]
    const post1 = incrementalPosts[i * 2]
    const post2 = incrementalPosts[i * 2 + 1]

    usersStream.sendData(
      v([i + 2]),
      new MultiSet([[[user.id, user] as [number, (typeof allUsers)[0]], 1]]),
    )
    postsStream.sendData(
      v([i + 2]),
      new MultiSet([
        [[post1.userId, post1] as [number, (typeof allPosts)[0]], 1],
        [[post2.userId, post2] as [number, (typeof allPosts)[0]], 1],
      ]),
    )
    graph.step()
  }

  db.close()
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
  .add('D2TS SQLite Join', () => {
    joinWithD2TSAndSQLite()
  })
  .on('cycle', (event: Benchmark.Event) => {
    console.log(String(event.target))
  })
  .on('complete', function (this: Benchmark.Suite) {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })
  .run()

// Benchmark suite for filtering
const filterSuite = new Benchmark.Suite('Incremental Filtering')

// Naive implementation
const naiveFilter = () => {
  let result = initialUsers.filter((user) => user.age > 30)
  
  let currentUsers = [...initialUsers]
  for (const user of incrementalUsers) {
    currentUsers.push(user)
    result = currentUsers.filter((user) => user.age > 30)
  }
  return result
}

// D2TS implementation
const filterWithD2TS = () => {
  const graph = new D2({ initialFrontier: v([0]) })
  const stream = graph.newInput<[number, (typeof allUsers)[0]]>()

  const filtered = stream.pipe(
    filter(([_key, user]: [number, (typeof allUsers)[0]]) => user.age > 30),
  )

  graph.finalize()

  // Send initial data
  stream.sendData(v([1]), initialUsersSet)
  graph.step()

  // Incrementally add remaining items
  for (let i = 0; i < incrementalUsers.length; i++) {
    const user = incrementalUsers[i]
    stream.sendData(
      v([i + 2]),
      new MultiSet([[[user.id, user] as [number, (typeof allUsers)[0]], 1]]),
    )
    graph.step()
  }

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
const mapSuite = new Benchmark.Suite('Incremental Mapping')

// Naive implementation
const naiveMap = () => {
  // Initial mapping with 900 items
  let result = initialUsers.map((user) => ({ name: user.name.toUpperCase() }))
  
  // Add one item at a time and recompute
  let currentUsers = [...initialUsers]
  for (const user of incrementalUsers) {
    currentUsers.push(user)
    result = currentUsers.map((user) => ({ name: user.name.toUpperCase() }))
  }
  return result
}

// D2TS implementation
const mapWithD2TS = () => {
  const graph = new D2({ initialFrontier: v([0]) })
  const stream = graph.newInput<[number, (typeof allUsers)[0]]>()

  const output = stream.pipe(
    map(([id, user]: [number, (typeof allUsers)[0]]) => [
      id,
      { name: user.name.toUpperCase() },
    ]),
  )

  graph.finalize()

  // Send initial data
  stream.sendData(v([1]), initialUsersSet)
  graph.step()

  // Incrementally add remaining items
  for (let i = 0; i < incrementalUsers.length; i++) {
    const user = incrementalUsers[i]
    stream.sendData(
      v([i + 2]),
      new MultiSet([[[user.id, user] as [number, (typeof allUsers)[0]], 1]]),
    )
    graph.step()
  }

  return output
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
  .on('complete', function (this: Benchmark.Suite) {
    console.log('Fastest is ' + this.filter('fastest').map('name'))
  })
  .run()
