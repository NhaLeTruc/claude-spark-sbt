#!/bin/bash
# Install git hooks for claude-spark-sbt

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_HOOKS_DIR="$(git rev-parse --git-dir)/hooks"

echo "Installing git hooks..."

# Install pre-commit hook
if [ -f "$SCRIPT_DIR/pre-commit" ]; then
    ln -sf "$SCRIPT_DIR/pre-commit" "$GIT_HOOKS_DIR/pre-commit"
    chmod +x "$GIT_HOOKS_DIR/pre-commit"
    echo "✓ Pre-commit hook installed"
else
    echo "✗ Pre-commit hook not found"
    exit 1
fi

echo "✅ Git hooks installed successfully!"
echo ""
echo "To skip hooks during commit, use: git commit --no-verify"
