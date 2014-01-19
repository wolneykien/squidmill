#!/bin/sh -eu

RELEASENUM=$(sed -n -e '/^Release:/ {s/^Release:[[:space:]]\+alt//p; q}' *.spec)
RELEASENUM=$((RELEASENUM + 1))
sed -i -e "s/^Release:.*$/Release: alt$RELEASENUM/" *.spec

add_changelog -e "- Rebuild with a new version of Gambit" *.spec
git add *.spec
gear-commit
