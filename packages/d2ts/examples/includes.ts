import { D2 } from '../src/index.js'
import { map, filter, join, concat, distinct, debug } from '../src/operators.js'
import { v } from '../src/order.js'

type Issue = {
  type: 'issue'
  id: number
  project_id: number
  title: string
  owner_id: number
}

type Comment = {
  type: 'comment'
  id: number
  issue_id: number
  text: string
  owner_id: number
}

type User = {
  id: number
  name: string
}

const graph = new D2({ initialFrontier: v([0, 0]) })

const inputUsers = graph.newInput<[number, User]>()
const inputIssues = graph.newInput<[number, Issue]>()
const inputComments = graph.newInput<[number, Comment]>()

// Transform comments into [issue_id, comment] pairs for joining
const commentsByIssue = inputComments.pipe(
  map(([id, comment]) => [comment.issue_id, comment] as [number, Comment])
)

// Issues for our project
const issuesForProject = inputIssues.pipe(
  filter(([id, issue]) => issue.project_id === 1)
)

// Issues ids for joining with comments
const issueIds = issuesForProject.pipe(
  map(([id, issue]) => [issue.id, undefined] as [number, undefined])
)

// Join comments and map back to just the comment
const commentsForProject = commentsByIssue.pipe(
  join(issueIds),
  map(([id, [comment, _]]) => [comment.id, comment] as [number, Comment])
)

// Users
const usersIdsForIssues = issuesForProject.pipe(
  map(([id, issue]) => [issue.owner_id, undefined] as [number, undefined])
)
const usersIdsForComments = commentsForProject.pipe(
  map(([id, comment]) => [comment.owner_id, undefined] as [number, undefined])
)
const usersIds = usersIdsForIssues.pipe(
  concat(usersIdsForComments)
)
const users = usersIds.pipe(
  join(inputUsers),
  map(([id, [_, user]]) => [id, user] as [number, User]),
  distinct()
)

// Concat comments and issues and output the result
const output = commentsForProject.pipe(
  concat(issuesForProject),
  concat(users),
  debug('output', true)
)

graph.finalize()

// Add some users
inputUsers.sendData(v([1, 0]), [
  [[1, { id: 1, name: 'Alice' }], 1],
  [[2, { id: 2, name: 'Bob' }], 1],
  [[3, { id: 3, name: 'Charlie' }], 1],
])

// Add some issues
inputIssues.sendData(v([1, 0]), [
  [[1, { type: 'issue', id: 1, project_id: 1, title: 'Issue 1', owner_id: 1 }], 1],
  [[2, { type: 'issue', id: 2, project_id: 2, title: 'Issue 2', owner_id: 2 }], 1],
  [[3, { type: 'issue', id: 3, project_id: 1, title: 'Issue 3', owner_id: 3 }], 1],
])

// Add some comments
inputComments.sendData(v([1, 0]), [
  [[1, { type: 'comment', id: 1, issue_id: 1, text: 'Comment 1', owner_id: 1 }], 1],
  [[2, { type: 'comment', id: 2, issue_id: 1, text: 'Comment 2', owner_id: 3 }], 1],
  [[3, { type: 'comment', id: 3, issue_id: 2, text: 'Comment 3', owner_id: 1 }], 1],
  [[4, { type: 'comment', id: 4, issue_id: 2, text: 'Comment 4', owner_id: 3 }], 1],
  [[5, { type: 'comment', id: 5, issue_id: 3, text: 'Comment 5', owner_id: 1 }], 1],
  [[6, { type: 'comment', id: 6, issue_id: 3, text: 'Comment 6', owner_id: 3 }], 1],
])

// Send frontiers
inputUsers.sendFrontier(v([2, 0]))
inputIssues.sendFrontier(v([2, 0]))
inputComments.sendFrontier(v([2, 0]))
graph.run()

// Add a new Comment to an issue in project 1
inputComments.sendData(v([2, 0]), [
  [[7, { type: 'comment', id: 7, issue_id: 1, text: 'Comment 7', owner_id: 1 }], 1],
])
inputUsers.sendFrontier(v([3, 0]))
inputIssues.sendFrontier(v([3, 0]))
inputComments.sendFrontier(v([3, 0]))
graph.run()

// Add a new Comment to an issue in project 2
inputComments.sendData(v([3, 0]), [
  [[8, { type: 'comment', id: 8, issue_id: 2, text: 'Comment 8', owner_id: 1 }], 1],
])
inputUsers.sendFrontier(v([4, 0]))
inputIssues.sendFrontier(v([4, 0]))
inputComments.sendFrontier(v([4, 0]))
graph.run()
console.log('> Comment 8 should not be included in the output above')

// Move issue 2 to project 1
inputIssues.sendData(v([4, 0]), [
  [[2, { type: 'issue', id: 2, project_id: 2, title: 'Issue 2', owner_id: 2 }], -1],
  [[2, { type: 'issue', id: 2, project_id: 1, title: 'Issue 2', owner_id: 2 }], 1],
])
inputUsers.sendFrontier(v([5, 0]))
inputIssues.sendFrontier(v([5, 0]))
inputComments.sendFrontier(v([5, 0]))
graph.run()
console.log('> Issue 2 and its comments should be included in the output above')

// Move issue 2 back to project 2
inputIssues.sendData(v([5, 0]), [
  [[2, { type: 'issue', id: 2, project_id: 1, title: 'Issue 2', owner_id: 2 }], -1],
  [[2, { type: 'issue', id: 2, project_id: 2, title: 'Issue 2', owner_id: 2 }], 1],
])
inputUsers.sendFrontier(v([6, 0]))
inputIssues.sendFrontier(v([6, 0]))
inputComments.sendFrontier(v([6, 0]))
graph.run()
console.log('> Issue 2 and its comments should have a multiplicity of -1 in the output above')

/*
Output looks like this:

debug output data: version: Version([1,0]) collection: MultiSet([
  [
    [
      1,
      {
        "type": "comment",
        "id": 1,
        "issue_id": 1,
        "text": "Comment 1",
        "owner_id": 1
      }
    ],
    1
  ],
  [
    [
      2,
      {
        "type": "comment",
        "id": 2,
        "issue_id": 1,
        "text": "Comment 2",
        "owner_id": 3
      }
    ],
    1
  ],
  [
    [
      5,
      {
        "type": "comment",
        "id": 5,
        "issue_id": 3,
        "text": "Comment 5",
        "owner_id": 1
      }
    ],
    1
  ],
  [
    [
      6,
      {
        "type": "comment",
        "id": 6,
        "issue_id": 3,
        "text": "Comment 6",
        "owner_id": 3
      }
    ],
    1
  ]
])
debug output data: version: Version([1,0]) collection: MultiSet([
  [
    [
      1,
      {
        "type": "issue",
        "id": 1,
        "project_id": 1,
        "title": "Issue 1",
        "owner_id": 1
      }
    ],
    1
  ],
  [
    [
      3,
      {
        "type": "issue",
        "id": 3,
        "project_id": 1,
        "title": "Issue 3",
        "owner_id": 3
      }
    ],
    1
  ]
])
debug output data: version: Version([1,0]) collection: MultiSet([
  [
    [
      1,
      {
        "id": 1,
        "name": "Alice"
      }
    ],
    1
  ],
  [
    [
      3,
      {
        "id": 3,
        "name": "Charlie"
      }
    ],
    1
  ]
])
debug output notification: frontier Antichain([[2,0]])
debug output data: version: Version([2,0]) collection: MultiSet([
  [
    [
      7,
      {
        "type": "comment",
        "id": 7,
        "issue_id": 1,
        "text": "Comment 7",
        "owner_id": 1
      }
    ],
    1
  ]
])
debug output notification: frontier Antichain([[3,0]])
debug output notification: frontier Antichain([[4,0]])
> Comment 8 should not be included in the output above
debug output data: version: Version([4,0]) collection: MultiSet([
  [
    [
      3,
      {
        "type": "comment",
        "id": 3,
        "issue_id": 2,
        "text": "Comment 3",
        "owner_id": 1
      }
    ],
    1
  ],
  [
    [
      4,
      {
        "type": "comment",
        "id": 4,
        "issue_id": 2,
        "text": "Comment 4",
        "owner_id": 3
      }
    ],
    1
  ],
  [
    [
      8,
      {
        "type": "comment",
        "id": 8,
        "issue_id": 2,
        "text": "Comment 8",
        "owner_id": 1
      }
    ],
    1
  ]
])
debug output data: version: Version([4,0]) collection: MultiSet([
  [
    [
      2,
      {
        "type": "issue",
        "id": 2,
        "project_id": 1,
        "title": "Issue 2",
        "owner_id": 2
      }
    ],
    1
  ]
])
debug output data: version: Version([4,0]) collection: MultiSet([
  [
    [
      2,
      {
        "id": 2,
        "name": "Bob"
      }
    ],
    1
  ]
])
debug output notification: frontier Antichain([[5,0]])
> Issue 2 and its comments should be included in the output above
debug output data: version: Version([5,0]) collection: MultiSet([
  [
    [
      3,
      {
        "type": "comment",
        "id": 3,
        "issue_id": 2,
        "text": "Comment 3",
        "owner_id": 1
      }
    ],
    -1
  ],
  [
    [
      4,
      {
        "type": "comment",
        "id": 4,
        "issue_id": 2,
        "text": "Comment 4",
        "owner_id": 3
      }
    ],
    -1
  ],
  [
    [
      8,
      {
        "type": "comment",
        "id": 8,
        "issue_id": 2,
        "text": "Comment 8",
        "owner_id": 1
      }
    ],
    -1
  ]
])
debug output data: version: Version([5,0]) collection: MultiSet([
  [
    [
      2,
      {
        "type": "issue",
        "id": 2,
        "project_id": 1,
        "title": "Issue 2",
        "owner_id": 2
      }
    ],
    -1
  ]
])
debug output data: version: Version([5,0]) collection: MultiSet([
  [
    [
      2,
      {
        "id": 2,
        "name": "Bob"
      }
    ],
    -1
  ]
])
debug output notification: frontier Antichain([[6,0]])
> Issue 2 and its comments should have a multiplicity of -1 in the output above

*/