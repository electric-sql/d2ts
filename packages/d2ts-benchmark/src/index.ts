import { Suite } from './base'
import { D2, MultiSet, map, join, filter, v, output } from 'd2ts'
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

function run({
  initialSize = 9000,
  incrementalRuns = 1000,
}: {
  initialSize: number
  incrementalRuns: number
}) {
  console.log('========================================')
  console.log(
    `Running with initialSize = ${initialSize}, incrementalRuns = ${incrementalRuns}`,
  )

  const totalSize = initialSize + incrementalRuns
  const { users: allUsers, posts: allPosts } = generateData(totalSize)

  const initialUsers = allUsers.slice(0, initialSize)
  const incrementalUsers = allUsers.slice(initialSize)
  const initialPosts = allPosts.slice(0, initialSize * 2)
  const incrementalPosts = allPosts.slice(initialSize * 2)

  // Convert initial arrays to MultiSets
  const initialUsersSet = new MultiSet(
    initialUsers.map((user) => [
      [user.id, user] as [number, (typeof allUsers)[0]],
      1,
    ]),
  )
  const initialPostsSet = new MultiSet(
    initialPosts.map((post) => [
      [post.userId, post] as [number, (typeof allPosts)[0]],
      1,
    ]),
  )

  const runOptions = {
    warmupRuns: 2,
    totalRuns: 5,
    incrementalRuns,
    maxTimeDoingIncrementalRuns: 1_000,
  }

  // Create and run the join suite
  const joinSuite = new Suite({
    name: 'Incremental Joins',
    ...runOptions,
  })

  // Add naive join benchmark
  // Skip for large datasets
  if (initialSize <= 9000) {
    joinSuite.add({
      name: 'Naive Join',
      setup: () => ({
        currentUsers: [...initialUsers],
        currentPosts: [...initialPosts],
        result: [] as { userName: string; postTitle: string }[],
      }),
      firstRun: (ctx) => {
        ctx.result = ctx.currentUsers.flatMap((user) =>
          ctx.currentPosts
            .filter((post) => post.userId === user.id)
            .map((post) => ({ userName: user.name, postTitle: post.title })),
        )
      },
      incrementalRun: (ctx, i) => {
        ctx.currentUsers.push(incrementalUsers[i])
        ctx.currentPosts.push(incrementalPosts[i * 2])
        ctx.currentPosts.push(incrementalPosts[i * 2 + 1])

        ctx.result = ctx.currentUsers.flatMap((user) =>
          ctx.currentPosts
            .filter((post) => post.userId === user.id)
            .map((post) => ({ userName: user.name, postTitle: post.title })),
        )
      },
    })
  }

  // Add naive indexed join benchmark
  joinSuite.add({
    name: 'Naive Indexed Join',
    setup: () => ({
      currentUsers: [...initialUsers],
      currentPosts: [...initialPosts],
      postsByUser: new Map<number, typeof initialPosts>(),
      result: [] as { userName: string; postTitle: string }[],
    }),
    firstRun: (ctx) => {
      // Build post index
      ctx.postsByUser.clear()
      for (const post of ctx.currentPosts) {
        if (!ctx.postsByUser.has(post.userId)) {
          ctx.postsByUser.set(post.userId, [])
        }
        ctx.postsByUser.get(post.userId)!.push(post)
      }

      // Compute join using index
      ctx.result = []
      for (const user of ctx.currentUsers) {
        const userPosts = ctx.postsByUser.get(user.id) || []
        for (const post of userPosts) {
          ctx.result.push({
            userName: user.name,
            postTitle: post.title,
          })
        }
      }
    },
    incrementalRun: (ctx, i) => {
      const user = incrementalUsers[i]
      const post1 = incrementalPosts[i * 2]
      const post2 = incrementalPosts[i * 2 + 1]

      // Update data
      ctx.currentUsers.push(user)
      ctx.currentPosts.push(post1, post2)

      // Rebuild post index from scratch
      ctx.postsByUser.clear()
      for (const post of ctx.currentPosts) {
        if (!ctx.postsByUser.has(post.userId)) {
          ctx.postsByUser.set(post.userId, [])
        }
        ctx.postsByUser.get(post.userId)!.push(post)
      }

      // Recompute entire join using index
      ctx.result = []
      for (const user of ctx.currentUsers) {
        const userPosts = ctx.postsByUser.get(user.id) || []
        for (const post of userPosts) {
          ctx.result.push({
            userName: user.name,
            postTitle: post.title,
          })
        }
      }
    },
  })

  // Add D2TS join benchmark
  joinSuite.add({
    name: 'D2TS Join',
    setup: () => {
      const graph = new D2({ initialFrontier: v([0]) })
      const usersStream = graph.newInput<[number, (typeof allUsers)[0]]>()
      const postsStream = graph.newInput<[number, (typeof allPosts)[0]]>()

      const joined = usersStream.pipe(
        join(postsStream),
        map(([_key, [user, post]]) => ({
          userName: user.name,
          postTitle: post.title,
        })),
        output((_data) => {
          // do nothing
        }),
      )

      graph.finalize()
      return { graph, usersStream, postsStream, joined }
    },
    firstRun: (ctx) => {
      ctx.usersStream.sendData(v([1]), initialUsersSet)
      ctx.postsStream.sendData(v([1]), initialPostsSet)
      ctx.graph.step()
    },
    incrementalRun: (ctx, i) => {
      const user = incrementalUsers[i]
      const post1 = incrementalPosts[i * 2]
      const post2 = incrementalPosts[i * 2 + 1]

      ctx.usersStream.sendData(v([i + 2]), new MultiSet([[[user.id, user], 1]]))
      ctx.postsStream.sendData(
        v([i + 2]),
        new MultiSet([
          [[post1.userId, post1], 1],
          [[post2.userId, post2], 1],
        ]),
      )
      ctx.graph.step()
    },
  })

  // Add D2TS join with frontier benchmark
  joinSuite.add({
    name: 'D2TS Join with Frontier',
    setup: () => {
      const graph = new D2({ initialFrontier: v([0]) })
      const usersStream = graph.newInput<[number, (typeof allUsers)[0]]>()
      const postsStream = graph.newInput<[number, (typeof allPosts)[0]]>()

      const joined = usersStream.pipe(
        join(postsStream),
        map(([_key, [user, post]]) => ({
          userName: user.name,
          postTitle: post.title,
        })),
        output((_data) => {
          // do nothing
        }),
      )

      graph.finalize()
      return { graph, usersStream, postsStream, joined }
    },
    firstRun: (ctx) => {
      ctx.usersStream.sendData(v([1]), initialUsersSet)
      ctx.postsStream.sendData(v([1]), initialPostsSet)
      ctx.usersStream.sendFrontier(v([2]))
      ctx.postsStream.sendFrontier(v([2]))
      ctx.graph.step()
    },
    incrementalRun: (ctx, i) => {
      const user = incrementalUsers[i]
      const post1 = incrementalPosts[i * 2]
      const post2 = incrementalPosts[i * 2 + 1]

      ctx.usersStream.sendData(v([i + 2]), new MultiSet([[[user.id, user], 1]]))
      ctx.postsStream.sendData(
        v([i + 2]),
        new MultiSet([
          [[post1.userId, post1], 1],
          [[post2.userId, post2], 1],
        ]),
      )
      ctx.usersStream.sendFrontier(v([i + 3]))
      ctx.postsStream.sendFrontier(v([i + 3]))
      ctx.graph.step()
    },
  })

  // Add SQLite join benchmark
  joinSuite.add({
    name: 'D2TS SQLite Join',
    setup: () => {
      const sqlite = new Database(':memory:')
      const db = new BetterSQLite3Wrapper(sqlite)

      // Improve the sqlite performance
      db.exec(`PRAGMA journal_mode = WAL;`)
      db.exec(`PRAGMA synchronous = OFF;`)
      db.exec(`PRAGMA temp_store = MEMORY;`)
      db.exec(`PRAGMA cache_size = -100000;`) // 100MB

      const graph = new D2({ initialFrontier: v([0]) })
      const usersStream = graph.newInput<[number, (typeof allUsers)[0]]>()
      const postsStream = graph.newInput<[number, (typeof allPosts)[0]]>()

      const joined = usersStream.pipe(
        joinSql(postsStream, db),
        map(([_key, [user, post]]) => ({
          userName: user.name,
          postTitle: post.title,
        })),
        output((_data) => {
          // do nothing
        }),
      )

      graph.finalize()
      return { graph, usersStream, postsStream, joined, db, sqlite }
    },
    firstRun: (ctx) => {
      ctx.usersStream.sendData(v([1]), initialUsersSet)
      ctx.postsStream.sendData(v([1]), initialPostsSet)
      ctx.graph.step()
    },
    incrementalRun: (ctx, i) => {
      const user = incrementalUsers[i]
      const post1 = incrementalPosts[i * 2]
      const post2 = incrementalPosts[i * 2 + 1]

      ctx.usersStream.sendData(v([i + 2]), new MultiSet([[[user.id, user], 1]]))
      ctx.postsStream.sendData(
        v([i + 2]),
        new MultiSet([
          [[post1.userId, post1], 1],
          [[post2.userId, post2], 1],
        ]),
      )
      ctx.graph.step()
    },
    teardown: (ctx) => {
      ctx.db.close()
      ctx.sqlite.close()
    },
  })

  // Add SQLite join with frontier benchmark
  // joinSuite.add({
  //   name: 'D2TS SQLite Join with Frontier',
  //   setup: () => {
  //     const sqlite = new Database(':memory:')
  //     const db = new BetterSQLite3Wrapper(sqlite)

  //     // Improve the sqlite performance
  //     db.exec(`PRAGMA journal_mode = WAL;`)
  //     db.exec(`PRAGMA synchronous = OFF;`)
  //     db.exec(`PRAGMA temp_store = MEMORY;`)
  //     db.exec(`PRAGMA cache_size = -100000;`) // 100MB

  //     const graph = new D2({ initialFrontier: v([0]) })
  //     const usersStream = graph.newInput<[number, (typeof allUsers)[0]]>()
  //     const postsStream = graph.newInput<[number, (typeof allPosts)[0]]>()

  //     const joined = usersStream.pipe(
  //       joinSql(postsStream, db),
  //       map(([_key, [user, post]]) => ({
  //         userName: user.name,
  //         postTitle: post.title,
  //       })),
  //       output((_data) => {
  //         // do nothing
  //       }),
  //     )

  //     graph.finalize()
  //     return { graph, usersStream, postsStream, joined, db, sqlite }
  //   },
  //   firstRun: (ctx) => {
  //     ctx.usersStream.sendData(v([1]), initialUsersSet)
  //     ctx.postsStream.sendData(v([1]), initialPostsSet)
  //     ctx.usersStream.sendFrontier(v([2]))
  //     ctx.postsStream.sendFrontier(v([2]))
  //     ctx.graph.step()
  //   },
  //   incrementalRun: (ctx, i) => {
  //     const user = incrementalUsers[i]
  //     const post1 = incrementalPosts[i * 2]
  //     const post2 = incrementalPosts[i * 2 + 1]

  //     ctx.usersStream.sendData(v([i + 2]), new MultiSet([[[user.id, user], 1]]))
  //     ctx.postsStream.sendData(
  //       v([i + 2]),
  //       new MultiSet([
  //         [[post1.userId, post1], 1],
  //         [[post2.userId, post2], 1],
  //       ]),
  //     )
  //     ctx.usersStream.sendFrontier(v([i + 3]))
  //     ctx.postsStream.sendFrontier(v([i + 3]))
  //     ctx.graph.step()
  //   },
  //   teardown: (ctx) => {
  //     ctx.db.close()
  //     ctx.sqlite.close()
  //   },
  // })

  joinSuite.run()
  joinSuite.printResults()

  // Create filter suite
  const filterSuite = new Suite({
    name: 'Incremental Filtering',
    ...runOptions,
  })

  // Add naive filter benchmark
  filterSuite.add({
    name: 'Naive Filter',
    setup: () => ({
      currentUsers: [...initialUsers],
      result: [] as { name: string; age: number }[],
    }),
    firstRun: (ctx) => {
      ctx.result = ctx.currentUsers.filter((user) => user.age > 30)
    },
    incrementalRun: (ctx, i) => {
      ctx.currentUsers.push(incrementalUsers[i])
      ctx.result = ctx.currentUsers.filter((user) => user.age > 30)
    },
  })

  // Add D2TS filter benchmark
  filterSuite.add({
    name: 'D2TS Filter',
    setup: () => {
      const graph = new D2({ initialFrontier: v([0]) })
      const stream = graph.newInput<[number, (typeof allUsers)[0]]>()
      stream.pipe(
        filter(([_key, user]) => user.age > 30),
        output((_data) => {
          // do nothing
        }),
      )
      graph.finalize()
      return { graph, stream }
    },
    firstRun: (ctx) => {
      ctx.stream.sendData(v([1]), initialUsersSet)
      ctx.graph.step()
    },
    incrementalRun: (ctx, i) => {
      const user = incrementalUsers[i]
      ctx.stream.sendData(v([i + 2]), new MultiSet([[[user.id, user], 1]]))
      ctx.graph.step()
    },
  })

  filterSuite.run()
  filterSuite.printResults()

  // Create map suite
  const mapSuite = new Suite({
    name: 'Incremental Mapping',
    ...runOptions,
  })

  // Add naive map benchmark
  mapSuite.add({
    name: 'Naive Map',
    setup: () => ({
      currentUsers: [...initialUsers],
      result: [] as { name: string }[],
    }),
    firstRun: (ctx) => {
      ctx.result = ctx.currentUsers.map((user) => ({
        name: user.name.toUpperCase(),
      }))
    },
    incrementalRun: (ctx, i) => {
      ctx.currentUsers.push(incrementalUsers[i])
      ctx.result = ctx.currentUsers.map((user) => ({
        name: user.name.toUpperCase(),
      }))
    },
  })

  // Add D2TS map benchmark
  mapSuite.add({
    name: 'D2TS Map',
    setup: () => {
      const graph = new D2({ initialFrontier: v([0]) })
      const stream = graph.newInput<[number, (typeof allUsers)[0]]>()
      stream.pipe(
        map(([id, user]) => [id, { name: user.name.toUpperCase() }]),
        output((_data) => {
          // do nothing
        }),
      )
      graph.finalize()
      return { graph, stream }
    },
    firstRun: (ctx) => {
      ctx.stream.sendData(v([1]), initialUsersSet)
      ctx.graph.step()
    },
    incrementalRun: (ctx, i) => {
      const user = incrementalUsers[i]
      ctx.stream.sendData(v([i + 2]), new MultiSet([[[user.id, user], 1]]))
      ctx.graph.step()
    },
  })

  mapSuite.run()
  mapSuite.printResults()
}

run({ initialSize: 90, incrementalRuns: 1000 })
run({ initialSize: 900, incrementalRuns: 1000 })
run({ initialSize: 9000, incrementalRuns: 1000 })
run({ initialSize: 90000, incrementalRuns: 1000 })
