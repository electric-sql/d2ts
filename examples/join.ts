import { D2, map, join, distinct, debug } from '@electric-sql/d2ts'

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
    title: 'Fix login bug',
    userId: 1,
  },
  {
    id: 2,
    title: 'Add dark mode',
    userId: 1,
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
    name: 'Alice Johnson',
  },
  {
    id: 2,
    name: 'Bob Smith',
  },
  {
    id: 3,
    name: 'Carol Williams',
  },
]

const graph = new D2({
  initialFrontier: 0,
})

const inputIssues = graph.newInput<[number, Issue]>()
const inputUsers = graph.newInput<[number, User]>()

// Transform issues into [key, value] pairs for joining
const issuesStream = inputIssues.pipe(
  // debug('issues_stream'),
  map(([issueId, issue]) => [issue.userId, issue] as [number, Issue]),
  // debug('issues_stream_map')
)

// Transform users into [key, value] pairs for joining
const usersStream = inputUsers.pipe(
  // debug('users_stream'),
  map(([userId, user]) => [userId, user] as [number, User]),
  // debug('users_stream_map')
)

// Join streams and transform to desired output format
const joinedStream = issuesStream.pipe(
  join(usersStream),
  // debug('join'),
  map(([_key, [issue, user]]) => [
    issue.id,
    {
      id: issue.id,
      title: issue.title,
      userName: user.name,
    },
  ]),
  debug('map', true),
  distinct(),
  debug('distinct', true),
)

graph.finalize()

for (const issue of issues) {
  inputIssues.sendData(1, [[[issue.id, issue], 1]])
}

for (const user of users) {
  inputUsers.sendData(1, [[[user.id, user], 1]])
}

inputIssues.sendFrontier(2)
inputUsers.sendFrontier(2)

graph.run()

// Add a new issue
inputIssues.sendData(2, [
  [
    [
      11,
      {
        id: 11,
        title: 'New issue',
        userId: 1,
      },
    ],
    1,
  ],
])

inputIssues.sendFrontier(3)
inputUsers.sendFrontier(3)

graph.run()

// Delete an issue
inputIssues.sendData(3, [
  [
    [
      1,
      {
        id: 1,
        title: 'Fix login bug',
        userId: 1,
      },
    ],
    -1,
  ],
])

inputIssues.sendFrontier(4)
inputUsers.sendFrontier(4)

graph.run()

// Insert a new user and issue by the same user
inputUsers.sendData(4, [
  [
    [
      4,
      {
        id: 4,
        name: 'Dave Brown',
      },
    ],
    1,
  ],
])

inputIssues.sendData(4, [
  [
    [
      12,
      {
        id: 12,
        title: 'New issue 2',
        userId: 4,
      },
    ],
    1,
  ],
])

inputIssues.sendFrontier(5)
inputUsers.sendFrontier(5)

graph.run()

// Delete a user and their issues
inputUsers.sendData(5, [
  [
    [
      4,
      {
        id: 4,
        name: 'Dave Brown',
      },
    ],
    -1,
  ],
])
inputIssues.sendData(5, [
  [
    [
      12,
      {
        id: 12,
        title: 'New issue 2',
        userId: 4,
      },
    ],
    -1,
  ],
])

inputIssues.sendFrontier(6)
inputUsers.sendFrontier(6)

graph.run()
