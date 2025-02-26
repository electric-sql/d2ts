import { MultiShapeStream } from '@electric-sql/experimental'
import { IssuePriority, IssueStatus, Issue, User, Comment } from './types'
import {
  D2,
  map,
  join,
  reduce,
  buffer,
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
// We need a zero for each issue to ensure that we get a row for each issue, even if there are no comments
const commentCountZero = issuesInput.pipe(
  map(([_key, issue]) => [issue.id, 0] as [string, number]),
)
const commentCounts = commentsInput.pipe(
  // debug('comments'),
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
  // debug('issues'),
  map(([_key, issue]) => [issue.user_id, issue] as [string, Issue]),
)

// Transform users for joining with issues
const usersForJoin = usersInput.pipe(
  // debug('users'),
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
  buffer(),
  // debug('output'),
  output((msg) => {
    if (msg.type === MessageType.DATA) {
      console.log('DATA version:', msg.data.version.toJSON())
      msg.data.collection.getInner().forEach(([[key, data], multiplicity]) => {
        if (multiplicity > 0) {
          console.log('+ Insert', data)
        } else if (multiplicity < 0) {
          console.log('- Delete', data)
        } else {
          throw new Error('Invalid multiplicity')
        }
      })
    } else if (msg.type === MessageType.FRONTIER) {
      console.log('FRONTIER', msg.data.toJSON())
    }
  }),
)

// Finalize graph
graph.finalize()

// Create Electric shape streams
// We are using the experimental MultiShapeStream to consume multiple shapes
// from the same Electric instance which ensures that we get an `up-to-date` on all
// shapes within the `checkForUpdatesAfter` interval.
const streams = new MultiShapeStream<{
  issue: Issue
  user: User
  comment: Comment
}>({
  checkForUpdatesAfterMs: 100,
  shapes: {
    issue: {
      url: ELECTRIC_URL,
      params: {
        table: 'issue',
        replica: 'full',
      },
    },
    user: {
      url: ELECTRIC_URL,
      params: {
        table: 'user',
        replica: 'full',
      },
    },
    comment: {
      url: ELECTRIC_URL,
      params: {
        table: 'comment',
        replica: 'full',
      },
    },
  },
})

// Connect Electric streams to D2 inputs

electricStreamToD2Input({
  graph,
  stream: streams.shapes.issue,
  input: issuesInput,
  runOn: 'lsn-advance',
  debug: (msg) => console.log('issue', msg),
})

electricStreamToD2Input({
  graph,
  stream: streams.shapes.user,
  input: usersInput,
  runOn: 'lsn-advance',
  debug: (msg) => console.log('user', msg),
})

electricStreamToD2Input({
  graph,
  stream: streams.shapes.comment,
  input: commentsInput,
  runOn: 'lsn-advance',
  debug: (msg) => console.log('comment', msg),
})
