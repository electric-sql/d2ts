import createPool, { sql } from '@databases/pg'
import { generateUsers, generateIssues } from './generate_data.js'

const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://postgres:password@localhost:54321/electric'
const ISSUES_TO_LOAD = process.env.ISSUES_TO_LOAD || 512
const USERS_TO_LOAD = process.env.USERS_TO_LOAD || 50

console.info(`Connecting to Postgres at ${DATABASE_URL}`)
const db = createPool(DATABASE_URL)

async function makeInsertQuery(db, table, data) {
  const columns = Object.keys(data)
  const columnsNames = columns.join(`, `)
  const values = columns.map((column) => data[column])
  return await db.query(sql`
    INSERT INTO ${sql.ident(table)} (${sql(columnsNames)})
    VALUES (${sql.join(values.map(sql.value), `, `)})
  `)
}

async function importUser(db, user) {
  return await makeInsertQuery(db, `user`, user)
}

async function importIssue(db, issue) {
  const { comments: _, ...rest } = issue
  return await makeInsertQuery(db, `issue`, rest)
}

async function importComment(db, comment) {
  return await makeInsertQuery(db, `comment`, comment)
}

// Generate and load users first
const users = generateUsers(USERS_TO_LOAD)
const userIds = users.map(user => user.id)

console.info(`Loading ${users.length} users...`)
await db.tx(async (db) => {
  for (const user of users) {
    await importUser(db, user)
  }
})

// Then generate and load issues with comments
const issues = generateIssues(ISSUES_TO_LOAD, userIds)
const issueCount = issues.length
let commentCount = 0
const batchSize = 100

for (let i = 0; i < issueCount; i += batchSize) {
  await db.tx(async (db) => {
    db.query(sql`SET CONSTRAINTS ALL DEFERRED;`) // disable FK checks
    for (let j = i; j < i + batchSize && j < issueCount; j++) {
      process.stdout.write(`Loading issue ${j + 1} of ${issueCount}\r`)
      const issue = issues[j]
      await importIssue(db, issue)
      for (const comment of issue.comments) {
        commentCount++
        await importComment(db, comment)
      }
    }
  })
}

process.stdout.write(`\n`)

await db.dispose()
console.info(`Loaded ${users.length} users, ${issueCount} issues with ${commentCount} comments.`)
