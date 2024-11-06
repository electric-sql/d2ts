import { DifferenceStreamBuilder, GraphBuilder } from './src/builder'
import { MultiSet } from './src/multiset'
import { Antichain, v } from './src/order'

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
    // description: "Users unable to login after password reset",
    user_id: 1
  },
  {
    id: 2, 
    title: "Add dark mode",
    // description: "Implement dark mode theme across app",
    user_id: 1
  },
  {
    id: 3,
    title: 'Performance optimization',
    // description: 'Improve page load times',
    user_id: 1,
  },
  {
    id: 4,
    title: 'Mobile responsiveness',
    // description: 'Fix layout issues on mobile devices',
    user_id: 2,
  },
  {
    id: 5,
    title: 'Update dependencies',
    // description: 'Update all npm packages to latest versions',
    user_id: 2,
  },
  {
    id: 6,
    title: 'Add unit tests',
    // description: 'Increase test coverage for core features',
    user_id: 2,
  },
  {
    id: 7,
    title: 'Fix typos in docs',
    // description: 'Correct documentation spelling errors',
    user_id: 2,
  },
  {
    id: 8,
    title: 'Add search feature',
    // description: 'Implement search functionality',
    user_id: 3,
  },
  {
    id: 9,
    title: 'Improve error handling',
    // description: 'Add better error messages and handling',
    user_id: 3,
  },
  {
    id: 10,
    title: 'Code cleanup',
    // description: 'Remove unused code and improve organization',
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

const graphBuilder = new GraphBuilder(
  new Antichain([v([0, 0])]),
)

const [input_issues, writer_issues] = graphBuilder.newInput<[number, Issue]>()
const [input_users, writer_users] = graphBuilder.newInput<[number, User]>()

// Transform issues into [key, value] pairs for joining
const issues_stream = input_issues
  // .debug('issues_stream')
  .map(([issue_id, issue]) => [issue.user_id, issue] as [number, Issue])
  // .debug('issues_stream_map')

// Transform users into [key, value] pairs for joining
const users_stream = input_users
  // .debug('users_stream')
  .map(([user_id, user]) => [user_id, user] as [number, User])
  // .debug('users_stream_map')

// Join streams and transform to desired output format
const joined_stream = issues_stream
  .join(users_stream)
  // .debug('join')
  .map(([_key, [issue, user]]) => ([issue.id, {
    id: issue.id,
    title: issue.title,
    // description: issue.description,
    user_name: user.name
  }]))
  .debug('map')

const graph = graphBuilder.finalize()

for (const issue of issues) {
  writer_issues.sendData(v([1, 0]), new MultiSet([[[issue.id, issue], 1]]))
}

for (const user of users) {
  writer_users.sendData(v([1, 0]), new MultiSet([[[user.id, user], 1]]))
}

writer_issues.sendFrontier(new Antichain([v([1, 0])]))
writer_users.sendFrontier(new Antichain([v([1, 0])]))

graph.step()

// Add a new issue
writer_issues.sendData(v([2, 0]), new MultiSet([[[11, {
  id: 11,
  title: 'New issue',
  user_id: 1,
}], 1]]))

writer_issues.sendFrontier(new Antichain([v([2, 0])]))
writer_users.sendFrontier(new Antichain([v([2, 0])]))

graph.step()

// Delete an issue
writer_issues.sendData(v([3, 0]), new MultiSet([[[1, {
  id: 1,
  title: 'Fix login bug',
  user_id: 1,
}], -1]]))

writer_issues.sendFrontier(new Antichain([v([3, 0])]))
writer_users.sendFrontier(new Antichain([v([3, 0])]))

graph.step()

/*
Output:

debug map data: version: Version([1,0]) collection: MultiSet([
  [
    [
      1,
      {
        "id": 1,
        "title": "Fix login bug",
        "user_name": "Alice Johnson"
      }
    ],
    1
  ],
  [
    [
      2,
      {
        "id": 2,
        "title": "Add dark mode",
        "user_name": "Alice Johnson"
      }
    ],
    1
  ],
  [
    [
      3,
      {
        "id": 3,
        "title": "Performance optimization",
        "user_name": "Alice Johnson"
      }
    ],
    1
  ],
  [
    [
      4,
      {
        "id": 4,
        "title": "Mobile responsiveness",
        "user_name": "Bob Smith"
      }
    ],
    1
  ],
  [
    [
      5,
      {
        "id": 5,
        "title": "Update dependencies",
        "user_name": "Bob Smith"
      }
    ],
    1
  ],
  [
    [
      6,
      {
        "id": 6,
        "title": "Add unit tests",
        "user_name": "Bob Smith"
      }
    ],
    1
  ],
  [
    [
      7,
      {
        "id": 7,
        "title": "Fix typos in docs",
        "user_name": "Bob Smith"
      }
    ],
    1
  ],
  [
    [
      8,
      {
        "id": 8,
        "title": "Add search feature",
        "user_name": "Carol Williams"
      }
    ],
    1
  ],
  [
    [
      9,
      {
        "id": 9,
        "title": "Improve error handling",
        "user_name": "Carol Williams"
      }
    ],
    1
  ],
  [
    [
      10,
      {
        "id": 10,
        "title": "Code cleanup",
        "user_name": "Carol Williams"
      }
    ],
    1
  ]
])
debug map notification: frontier Antichain([[1,0]])
debug map data: version: Version([2,0]) collection: MultiSet([
  [
    [
      11,
      {
        "id": 11,
        "title": "New issue",
        "user_name": "Alice Johnson"
      }
    ],
    1
  ]
])
debug map notification: frontier Antichain([[2,0]])
debug map data: version: Version([3,0]) collection: MultiSet([
  [
    [
      1,
      {
        "id": 1,
        "title": "Fix login bug",
        "user_name": "Alice Johnson"
      }
    ],
    -1
  ]
])
debug map notification: frontier Antichain([[3,0]])


*/