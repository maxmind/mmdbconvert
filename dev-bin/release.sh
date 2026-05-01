#!/bin/bash

set -eu -o pipefail

cd "$(git rev-parse --show-toplevel)"

# Pre-flight checks
check_command() {
    if ! command -v "$1" &>/dev/null; then
        echo "Error: $1 is not installed or not in PATH"
        exit 1
    fi
}

check_command go
check_command gh
check_command sed

# Verify gh CLI is authenticated
if ! gh auth status &>/dev/null; then
    echo "Error: gh CLI is not authenticated. Run 'gh auth login' first."
    exit 1
fi

# Fetch latest changes and check that we're not behind origin/main
echo "Fetching from origin..."
git fetch origin

# Check that we're not on the main branch
current_branch=$(git branch --show-current)
if [ "$current_branch" = "main" ]; then
    echo "Error: Releases should not be done directly on the main branch."
    echo "Please create a release branch and run this script from there."
    exit 1
fi

if ! git merge-base --is-ancestor origin/main HEAD; then
    echo "Error: Current branch is behind origin/main."
    echo "Please merge or rebase with origin/main before releasing."
    exit 1
fi

changelog=$(cat CHANGELOG.md)

# Regex for Keep a Changelog format: ## [0.2.0] - 2026-05-01
# We use a literal newline in the regex to match across multiple lines
regex='
## \[([0-9]+\.[0-9]+\.[0-9]+(-[^ ]+)?)\] - ([0-9]{4}-[0-9]{2}-[0-9]{2})

((.|
)*)
'

if [[ ! $changelog =~ $regex ]]; then
    echo "Could not find version line in CHANGELOG.md!"
    echo "Expected format: ## [X.Y.Z] - YYYY-MM-DD"
    exit 1
fi

version="${BASH_REMATCH[1]}"
date="${BASH_REMATCH[3]}"
# Extract notes until the next version header
notes="$(echo "${BASH_REMATCH[4]}" | sed -n -E '/^## \[?[0-9]+\.[0-9]+\.[0-9]+/,$!p')"
tag="v$version"

if [[ "$date" != "$(date +"%Y-%m-%d")" ]]; then
    echo "$date is not today!"
    exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
    echo ". is not clean." >&2
    exit 1
fi

echo $'\nVersion:'
echo "$version"

echo $'\nRelease notes:'
echo "$notes"

read -r -e -p "Continue? (y/n) " ok

if [ "$ok" != "y" ]; then
    echo "Aborting"
    exit 1
fi

git push

echo "Creating tag $tag"

# The message for the annotated tag is the version and the release notes
message="$version

$notes"

git tag -a -m "$message" "$tag"

git push --tags

gh release create --target "$current_branch" -t "$version" -n "$notes" "$tag"
