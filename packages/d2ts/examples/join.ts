import { D2 } from '../src/index.js'
import { map, join, distinct, debug } from '../src/operators/index.js'
import { v } from '../src/order.js'
import { parseArgs } from 'node:util'

type Issue = {
  id: number
  title: string
  // description: string
  userId: number
}

type User = {
  id: number
  name: string
}

const issues: Issue[] = [
  {
    id: 1,
    title: "Fix login bug",
    userId: 1
  },
  {
    id: 2, 
    title: "Add dark mode",
    userId: 1
  },
  {
    id: 3,
    title: 'Performance optimization',
    userId: 1,
  },
  {
    id: 4,
    title: 'Mobile responsiveness',
    userId: 2,
  },
  {
    id: 5,
    title: 'Update dependencies',
    userId: 2,
  },
  {
    id: 6,
    title: 'Add unit tests',
    userId: 2,
  },
  {
    id: 7,
    title: 'Fix typos in docs',
    userId: 2,
  },
  {
    id: 8,
    title: 'Add search feature',
    userId: 3,
  },
  {
    id: 9,
    title: 'Improve error handling',
    userId: 3,
  },
  {
    id: 10,
    title: 'Code cleanup',
    userId: 3,
  },
]

const users: User[] = [
  {
    id: 1,
    name: "Alice Johnson"
  },
  {
    id: 2,
    name: "Bob Smith"
  },
  {
    id: 3,
    name: "Carol Williams"
  }
]

const args = parseArgs({
  options: {
    sqlite: {
      type: 'boolean',
      short: 's',
      default: false
    }
  }
})

const graph = new D2({ 
  initialFrontier: v([0, 0]),
})

const inputIssues = graph.newInput<[number, Issue]>()
const inputUsers = graph.newInput<[number, User]>()

// Transform issues into [key, value] pairs for joining
const issuesStream = inputIssues.pipe(
  // debug('issues_stream'),
  map(([issueId, issue]) => [issue.userId, issue] as [number, Issue])
  // debug('issues_stream_map')
)

// Transform users into [key, value] pairs for joining
const usersStream = inputUsers.pipe(
  // debug('users_stream'),
  map(([userId, user]) => [userId, user] as [number, User])
  // debug('users_stream_map')
)

// Join streams and transform to desired output format
const joinedStream = issuesStream.pipe(
  join(usersStream),
  // debug('join'),
  map(([_key, [issue, user]]) => ([issue.id, {
    id: issue.id,
    title: issue.title,
    userName: user.name
  }])),
  debug('map', true),
  distinct(),
  debug('distinct', true)
)

graph.finalize()

for (const issue of issues) {
  inputIssues.sendData(v([1, 0]), [[[issue.id, issue], 1]])
}

for (const user of users) {
  inputUsers.sendData(v([1, 0]), [[[user.id, user], 1]])
}

inputIssues.sendFrontier(v([2, 0]))
inputUsers.sendFrontier(v([2, 0]))

graph.run()

// Add a new issue
inputIssues.sendData(v([2, 0]), [[[11, {
  id: 11,
  title: 'New issue',
  userId: 1,
}], 1]])

inputIssues.sendFrontier(v([3, 0]))
inputUsers.sendFrontier(v([3, 0]))

graph.run()

// Delete an issue
inputIssues.sendData(v([3, 0]), [[[1, {
  id: 1,
  title: 'Fix login bug',
  userId: 1,
}], -1]])

inputIssues.sendFrontier(v([4, 0]))
inputUsers.sendFrontier(v([4, 0]))

graph.run()

// Insert a new user and issue by the same user
inputUsers.sendData(v([4, 0]), [[[4, {
  id: 4,
  name: 'Dave Brown',
}], 1]])

inputIssues.sendData(v([4, 0]), [[[12, {
  id: 12,
  title: 'New issue 2',
  userId: 4,
}], 1]])

inputIssues.sendFrontier(v([5, 0]))
inputUsers.sendFrontier(v([5, 0]))

graph.run()

// Delete a user and their issues
inputUsers.sendData(v([5, 0]), [[[4, {
  id: 4,
  name: 'Dave Brown',
}], -1]])
inputIssues.sendData(v([5, 0]), [[[12, {
  id: 12,
  title: 'New issue 2',
  userId: 4,
}], -1]])

inputIssues.sendFrontier(v([6, 0]))
inputUsers.sendFrontier(v([6, 0]))

graph.run()