# Releasing

- Install `bash`, `git`, `go`, `gh`, and `sed`. Authenticate with
  `gh auth login` and ensure your Git credentials can push to the repository.
- Create a release branch from `main`.
- Update the version, date, and notes in `CHANGELOG.md`. Use the format
  `## [0.2.2] - YYYY-MM-DD` with today's date and a semantic version.
- Commit the changes.
- Run `dev-bin/release.sh` and review the version and notes before confirming.
  The script pushes the branch, then pushes an annotated tag containing the
  release notes.
- An authorized releaser must approve the Release workflow's pending deployment.
  GoReleaser uploads the binaries and packages to a draft release, then
  publishes it with the notes from the tag.
- Verify the assets and notes on the GitHub Releases page.
- Open a PR for the release branch and get it merged.
