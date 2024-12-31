CREATE TABLE IF NOT EXISTS "user" (
    "id" UUID NOT NULL,
    "username" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "full_name" TEXT NOT NULL,
    CONSTRAINT "user_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "issue" (
    "id" UUID NOT NULL,
    "title" TEXT NOT NULL,    
    "description" TEXT NOT NULL,
    "priority" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "modified" TIMESTAMPTZ NOT NULL,
    "created" TIMESTAMPTZ NOT NULL,
    "user_id" UUID NOT NULL,
    CONSTRAINT "issue_pkey" PRIMARY KEY ("id"),
    FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE DEFERRABLE
);

CREATE TABLE  IF NOT EXISTS "comment" (
    "id" UUID NOT NULL,
    "body" TEXT NOT NULL,
    "user_id" UUID NOT NULL,
    "issue_id" UUID NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL,
    CONSTRAINT "comment_pkey" PRIMARY KEY ("id"),
    FOREIGN KEY (user_id) REFERENCES "user"(id) ON DELETE CASCADE DEFERRABLE,
    FOREIGN KEY (issue_id) REFERENCES issue(id) ON DELETE CASCADE DEFERRABLE
);