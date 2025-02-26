# Release Process

This repository uses [Changesets](https://github.com/changesets/changesets) to manage versions, create changelogs, and publish to npm.

## Workflow

1. Make changes
2. Add a changeset using `pnpm changeset`
3. Commit and push
4. Create a PR
5. When the PR is merged, a "Version Packages" PR will be automatically created
6. When the "Version Packages" PR is merged, packages will be published to npm automatically

## Creating a changeset

To create a changeset, run:

```bash
pnpm changeset
```

This will:
1. Ask which packages you want to release
2. Ask what type of version change it is (major, minor, patch)
3. Ask for a summary of the changes

This will create a markdown file in the `.changeset` directory that should be committed with your changes.

## CI & Release Process

The CI/Release process is streamlined into a single workflow:

1. All PRs and main branch commits trigger the CI process (build, type check, lint, test)
2. When changes are merged to the main branch, the same workflow also:
   - Creates or updates a "Version Packages" PR that includes version bumps and changelog updates
   - When the Version Packages PR is merged, automatically publishes packages to npm

## Manual release

If you need to do a manual release:

1. Run `pnpm changeset version` to update versions and changelogs
2. Run `pnpm build` to build all packages
3. Run `pnpm changeset publish` to publish to npm 
