import { GraphBuilder } from '../src/builder'
import { MultiSet } from '../src/multiset'
import { Antichain, v } from '../src/order'

type Issue = {
  type: 'issue'
  id: number
  project_id: number // Foreign key to Project (not included in this example, but filtered on)
  title: string
  owner_id: number // Foreign key to User
}

type Comment = {
  type: 'comment'
  id: number
  issue_id: number // Foreign key to Issue
  text: string
  owner_id: number // Foreign key to User
}

type User = {
  id: number
  name: string
}

const graphBuilder = new GraphBuilder(new Antichain([v([0, 0])]))

const [inputUsers, writerUsers] = graphBuilder.newInput<[number, User]>()
const [inputIssues, writerIssues] = graphBuilder.newInput<[number, Issue]>()
const [inputComments, writerComments] =
  graphBuilder.newInput<[number, Comment]>()

// Transform comments into [issue_id, comment] pairs for joining
const commentsByIssue = inputComments.map(
  ([id, comment]) => [comment.issue_id, comment] as [number, Comment],
)

// Issues for our project
const issuesForProject = inputIssues.filter(
  ([id, issue]) => issue.project_id === 1,
)

// Issues ids - we don't need the issue data, just the ids, for joining with comments
// I think this should make for a smaller index inside the join operator
const issueIds = issuesForProject.map(
  ([id, issue]) => [issue.id, undefined] as [number, undefined],
)

// Join comments and map back to just the comment
const commentsForProject = commentsByIssue
  .join(issueIds)
  .map(([id, [comment, _]]) => [comment.id, comment] as [number, Comment])

// Users
const usersIdsForIssues = issuesForProject.map(
  ([id, issue]) => [issue.owner_id, undefined] as [number, undefined],
)
const usersIdsForComments = commentsForProject.map(
  ([id, comment]) => [comment.owner_id, undefined] as [number, undefined],
)
const usersIds = usersIdsForIssues.concat(usersIdsForComments)
const users = usersIds
  .join(inputUsers)
  .map(([id, [_, user]]) => [id, user] as [number, User])
  .distinct()

// Concat comments and issues and output the result
const output = commentsForProject.concat(issuesForProject).concat(users)

// Console.log the output
output.debug('output', true)

const graph = graphBuilder.finalize()

// Add some users
writerUsers.sendData(v([1, 0]), new MultiSet([
  [[1, { id: 1, name: 'Alice' }], 1],
  [[2, { id: 2, name: 'Bob' }], 1],
  [[3, { id: 3, name: 'Charlie' }], 1],
]))

// Add some issues
writerIssues.sendData(
  v([1, 0]),
  new MultiSet([
    [[1, { type: 'issue', id: 1, project_id: 1, title: 'Issue 1', owner_id: 1 }], 1],
    [[2, { type: 'issue', id: 2, project_id: 2, title: 'Issue 2', owner_id: 2 }], 1],
    [[3, { type: 'issue', id: 3, project_id: 1, title: 'Issue 3', owner_id: 3 }], 1],
  ]),
)

// Add some comments
writerComments.sendData(
  v([1, 0]),
  new MultiSet([
    [[1, { type: 'comment', id: 1, issue_id: 1, text: 'Comment 1', owner_id: 1 }], 1],
    [[2, { type: 'comment', id: 2, issue_id: 1, text: 'Comment 2', owner_id: 3 }], 1],
    [[3, { type: 'comment', id: 3, issue_id: 2, text: 'Comment 3', owner_id: 1 }], 1],
    [[4, { type: 'comment', id: 4, issue_id: 2, text: 'Comment 4', owner_id: 3 }], 1],
    [[5, { type: 'comment', id: 5, issue_id: 3, text: 'Comment 5', owner_id: 1 }], 1],
    [[6, { type: 'comment', id: 6, issue_id: 3, text: 'Comment 6', owner_id: 3 }], 1],
  ]),
)

// Send frontiers
writerUsers.sendFrontier(new Antichain([v([2, 0])]))
writerIssues.sendFrontier(new Antichain([v([2, 0])]))
writerComments.sendFrontier(new Antichain([v([2, 0])]))

// Step the graph
graph.step()

// Add a new Comment to an issue in project 1, send frontier and step
writerComments.sendData(
  v([2, 0]),
  new MultiSet([
    [[7, { type: 'comment', id: 7, issue_id: 1, text: 'Comment 7', owner_id: 1 }], 1],
  ]),
)
writerUsers.sendFrontier(new Antichain([v([3, 0])]))
writerIssues.sendFrontier(new Antichain([v([3, 0])]))
writerComments.sendFrontier(new Antichain([v([3, 0])]))
graph.step()

// Add a new Comment to an issue in project 2, send frontier and step
writerComments.sendData(
  v([3, 0]),
  new MultiSet([
    [[8, { type: 'comment', id: 8, issue_id: 2, text: 'Comment 8', owner_id: 1 }], 1],
  ]),
)
writerUsers.sendFrontier(new Antichain([v([4, 0])]))
writerIssues.sendFrontier(new Antichain([v([4, 0])]))
writerComments.sendFrontier(new Antichain([v([4, 0])]))
graph.step()
console.log('> Comment 8 should not be included in the output above')

// Move issue 2 to project 1, send frontier and step
// Updates in differential dataflow are done by removing the old value and adding the new value
writerIssues.sendData(
  v([4, 0]),
  new MultiSet([
    [[2, { type: 'issue', id: 2, project_id: 2, title: 'Issue 2', owner_id: 2 }], -1], // Remove
    [[2, { type: 'issue', id: 2, project_id: 1, title: 'Issue 2', owner_id: 2 }], 1], // Add
  ]),
)
writerUsers.sendFrontier(new Antichain([v([5, 0])]))
writerIssues.sendFrontier(new Antichain([v([5, 0])]))
writerComments.sendFrontier(new Antichain([v([5, 0])]))
graph.step()
console.log('> Issue 2 and its comments should be included in the output above')

// Move issue 2 back to project 2, send frontier and step
writerIssues.sendData(
  v([5, 0]),
  new MultiSet([
    [[2, { type: 'issue', id: 2, project_id: 1, title: 'Issue 2', owner_id: 2 }], -1], // Remove
    [[2, { type: 'issue', id: 2, project_id: 2, title: 'Issue 2', owner_id: 2 }], 1], // Add
  ]),
)
writerUsers.sendFrontier(new Antichain([v([6, 0])]))
writerIssues.sendFrontier(new Antichain([v([6, 0])]))
writerComments.sendFrontier(new Antichain([v([6, 0])]))
graph.step()
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