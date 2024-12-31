import { Row, ShapeStream, isControlMessage } from '@electric-sql/client'
import { IssuePriority, IssueStatus, Issue, User, Comment } from './types'
import {
  D2,
  map,
  join,
  reduce,
  consolidate,
  debug,
  output,
  MessageType,
  concat,
} from 'd2ts'
import { electricStreamToD2Input } from 'd2ts/electric'

const ELECTRIC_URL = 'http://localhost:3000/v1/shape'

export interface IssueData {
  id: string
  title: string
  description: string
  priority: IssuePriority
  status: IssueStatus
  modified: Date
  created: Date
  user_id: string
  username: string
  user_email: string
  user_full_name: string
  comment_count: number
}

// Create D2 graph
const graph = new D2({ initialFrontier: 0 })

// Create D2 inputs
const issuesInput = graph.newInput<[string, Issue]>()
const usersInput = graph.newInput<[string, User]>()
const commentsInput = graph.newInput<[string, Comment]>()

// issuesInput.pipe(debug('issues'))

// Calculate comment counts per issue
// The join we use later is a full outer join, so we need to add a zero for each issue
// to ensure that we get a row for each issue, even if there are no comments
const commentCountZero = issuesInput.pipe(
  map(([_key, issue]) => [issue.id, 0] as [string, number]),
)
const commentCounts = commentsInput.pipe(
  debug('comments'),
  map(([_key, comment]) => [comment.issue_id, 1] as [string, number]),
  concat(commentCountZero),
  reduce((values) => {
    let count = 0
    for (const [num, diff] of values) {
      count += num * diff
    }
    return [[count, 1]]
  }),
)

// Transform issues for joining with users
const issuesForJoin = issuesInput.pipe(
  debug('issues'),
  map(([_key, issue]) => [issue.user_id, issue] as [string, Issue]),
)

// Transform users for joining with issues
const usersForJoin = usersInput.pipe(
  debug('users'),
  map(([_key, user]) => [user.id, user] as [string, User]),
)

// Join issues with users
const issuesWithUsers = issuesForJoin.pipe(
  join(usersForJoin),
  map(
    ([_key, [issue, user]]) =>
      [issue.id, { issue, user }] as [string, { issue: Issue; user: User }],
  ),
)

// Join with comment counts and map to final structure
const finalStream = issuesWithUsers.pipe(
  join(commentCounts),
  map(([_key, [data, commentCount]]) => {
    const { issue, user } = data
    return [
      issue.id,
      {
        id: issue.id,
        title: issue.title,
        description: issue.description,
        priority: issue.priority,
        status: issue.status,
        modified: issue.modified,
        created: issue.created,
        user_id: user.id,
        username: user.username,
        user_email: user.email,
        user_full_name: user.full_name,
        comment_count: commentCount || 0,
      } as IssueData,
    ]
  }),
  consolidate(),
  debug('output'),
  output((msg) => {
    if (msg.type === MessageType.DATA) {
      msg.data.collection.getInner().forEach(([[key, data], multiplicity]) => {
        if (multiplicity > 0) {
          console.log('+ Insert', data)
        } else if (multiplicity < 0) {
          console.log('- Delete', data)
        }
      })
    }
  }),
)

// Finalize graph
graph.finalize()

// Create Electric shape streams
const issuesStream = new ShapeStream<Issue>({
  url: ELECTRIC_URL,
  params: {
    table: 'issue',
    replica: 'full',
  },
})

const usersStream = new ShapeStream<User>({
  url: ELECTRIC_URL,
  params: {
    table: 'user',
    replica: 'full',
  },
})

const commentsStream = new ShapeStream<Comment>({
  url: ELECTRIC_URL,
  params: {
    table: 'comment',
    replica: 'full',
  },
})

// Connect Electric streams to D2 inputs

const MAX = Number.MAX_VALUE

electricStreamToD2Input({
  graph,
  stream: issuesStream,
  input: issuesInput,
  runOn: 'lsn-advance',
})

electricStreamToD2Input({
  graph,
  stream: usersStream,
  input: usersInput,
  runOn: 'lsn-advance',
})

electricStreamToD2Input({
  graph,
  stream: commentsStream,
  input: commentsInput,
  runOn: 'lsn-advance',
})
