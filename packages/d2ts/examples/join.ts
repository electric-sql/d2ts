import { D2 } from '../src/pipe'
import { map, join, distinct, debug } from '../src/operators'
import { v } from '../src/order'
import { parseArgs } from 'node:util'

type Issue = {
  id: number
  title: string
  // description: string
  user_id: number
}

type User = {
  id: number
  name: string
}

const issues: Issue[] = [
  {
    id: 1,
    title: "Fix login bug",
    user_id: 1
  },
  {
    id: 2, 
    title: "Add dark mode",
    user_id: 1
  },
  {
    id: 3,
    title: 'Performance optimization',
    user_id: 1,
  },
  {
    id: 4,
    title: 'Mobile responsiveness',
    user_id: 2,
  },
  {
    id: 5,
    title: 'Update dependencies',
    user_id: 2,
  },
  {
    id: 6,
    title: 'Add unit tests',
    user_id: 2,
  },
  {
    id: 7,
    title: 'Fix typos in docs',
    user_id: 2,
  },
  {
    id: 8,
    title: 'Add search feature',
    user_id: 3,
  },
  {
    id: 9,
    title: 'Improve error handling',
    user_id: 3,
  },
  {
    id: 10,
    title: 'Code cleanup',
    user_id: 3,
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

const input_issues = graph.newInput<[number, Issue]>()
const input_users = graph.newInput<[number, User]>()

// Transform issues into [key, value] pairs for joining
const issues_stream = input_issues.pipe(
  // debug('issues_stream'),
  map(([issue_id, issue]) => [issue.user_id, issue] as [number, Issue])
  // debug('issues_stream_map')
)

// Transform users into [key, value] pairs for joining
const users_stream = input_users.pipe(
  // debug('users_stream'),
  map(([user_id, user]) => [user_id, user] as [number, User])
  // debug('users_stream_map')
)

// Join streams and transform to desired output format
const joined_stream = issues_stream.pipe(
  join(users_stream),
  // debug('join'),
  map(([_key, [issue, user]]) => ([issue.id, {
    id: issue.id,
    title: issue.title,
    user_name: user.name
  }])),
  debug('map', true),
  distinct(),
  debug('distinct', true)
)

graph.finalize()

for (const issue of issues) {
  input_issues.sendData(v([1, 0]), [[[issue.id, issue], 1]])
}

for (const user of users) {
  input_users.sendData(v([1, 0]), [[[user.id, user], 1]])
}

input_issues.sendFrontier(v([2, 0]))
input_users.sendFrontier(v([2, 0]))

graph.step()

// Add a new issue
input_issues.sendData(v([2, 0]), [[[11, {
  id: 11,
  title: 'New issue',
  user_id: 1,
}], 1]])

input_issues.sendFrontier(v([3, 0]))
input_users.sendFrontier(v([3, 0]))

graph.step()

// Delete an issue
input_issues.sendData(v([3, 0]), [[[1, {
  id: 1,
  title: 'Fix login bug',
  user_id: 1,
}], -1]])

input_issues.sendFrontier(v([4, 0]))
input_users.sendFrontier(v([4, 0]))

graph.step()

// Insert a new user and issue by the same user
input_users.sendData(v([4, 0]), [[[4, {
  id: 4,
  name: 'Dave Brown',
}], 1]])

input_issues.sendData(v([4, 0]), [[[12, {
  id: 12,
  title: 'New issue 2',
  user_id: 4,
}], 1]])

input_issues.sendFrontier(v([5, 0]))
input_users.sendFrontier(v([5, 0]))

graph.step()

// Delete a user and their issues
input_users.sendData(v([5, 0]), [[[4, {
  id: 4,
  name: 'Dave Brown',
}], -1]])
input_issues.sendData(v([5, 0]), [[[12, {
  id: 12,
  title: 'New issue 2',
  user_id: 4,
}], -1]])

input_issues.sendFrontier(v([6, 0]))
input_users.sendFrontier(v([6, 0]))

graph.step()