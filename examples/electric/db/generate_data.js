import { faker } from '@faker-js/faker'
import { v4 as uuidv4 } from 'uuid'

export function generateUsers(numUsers) {
  return Array.from({ length: numUsers }, generateUser)
}

function generateUser() {
  return {
    id: uuidv4(),
    username: faker.internet.userName(),
    email: faker.internet.email(),
    full_name: faker.person.fullName()
  }
}

export function generateIssues(numIssues, userIds) {
  if (!userIds?.length) {
    throw new Error('User IDs are required to generate issues')
  }
  
  return Array.from({ length: numIssues }, () => generateIssue(userIds))
}

function generateIssue(userIds) {
  const issueId = uuidv4()
  const createdAt = faker.date.past()
  return {
    id: issueId,
    title: faker.lorem.sentence({ min: 3, max: 8 }),
    description: faker.lorem.sentences({ min: 2, max: 6 }, `\n`),
    priority: faker.helpers.arrayElement([`none`, `low`, `medium`, `high`]),
    status: faker.helpers.arrayElement([
      `backlog`,
      `todo`,
      `in_progress`,
      `done`,
      `canceled`,
    ]),
    created: createdAt.toISOString(),
    modified: faker.date
      .between({ from: createdAt, to: new Date() })
      .toISOString(),
    user_id: faker.helpers.arrayElement(userIds),
    comments: faker.helpers.multiple(
      () => generateComment(issueId, createdAt, userIds),
      { count: faker.number.int({ min: 0, max: 10 }) }
    ),
  }
}

function generateComment(issueId, issueCreatedAt, userIds) {
  return {
    id: uuidv4(),
    body: faker.lorem.text(),
    user_id: faker.helpers.arrayElement(userIds),
    issue_id: issueId,
    created_at: faker.date
      .between({ from: issueCreatedAt, to: new Date() })
      .toISOString(),
  }
}
