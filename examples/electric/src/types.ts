export interface User extends Record<string, unknown> {
  id: string // UUID
  username: string
  email: string
  full_name: string
}

export interface Issue extends Record<string, unknown> {
  id: string // UUID
  title: string
  description: string
  priority: IssuePriority
  status: IssueStatus
  modified: Date
  created: Date
  user_id: string // UUID, foreign key to User
}

export interface Comment extends Record<string, unknown> {
  id: string // UUID
  body: string
  user_id: string // UUID, foreign key to User
  issue_id: string // UUID, foreign key to Issue
  created_at: Date
}

export const IssuePriority = {
  none: 'none',
  low: 'low',
  medium: 'medium',
  high: 'high',
} as const

export type IssuePriority = (typeof IssuePriority)[keyof typeof IssuePriority]

export const IssueStatus = {
  backlog: 'backlog',
  todo: 'todo',
  in_progress: 'in_progress',
  done: 'done',
  canceled: 'canceled',
} as const

export type IssueStatus = (typeof IssueStatus)[keyof typeof IssueStatus]
