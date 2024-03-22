## Configure the Git hooks for local CloudberryDB development

Git hooks are scripts that run automatically every time a particular
Git event occurs in a Git repository. We customize our CloudberryDB
Git’s behavior by these hooks to check or remind us how to improve our
code and commit.

## List of hooks

| Type | Description |
|--|--|
| `pre-commit` | This hook is to verify what is about to be committed and executed every time you run git commit before writing a commit message, including if the code has been properly indented and if has a trailing whitespace. |
| `prepare-commit-msg` | This hook is called after the `pre-commit` hook to populate the text editor with a commit message template.  |
| `commit-msg` | This hook is to check the commit log message and called after the user enters a commit message.  |
| `pre-push` | This hook is to verify what is about to be pushed, and called by `git push` after it has checked the remote status, but before anything has been pushed.  |

## Install

You can configure these Git hooks in one of the following three ways
as you like:

* Run the command to configure the file path for Git hooks:

```
# Run the command under src/tools/hooks dir

git config core.hooksPath $PWD
```

* Or link the hook files to the `.git/hooks` relative files:

```
# Run the command under src/tool/hooks dir

ln -sf ./pre-commit ../../../.git/hooks/pre-commit
ln -sf ./prepare-commit-msg ../../../.git/hooks/prepare-commit-msg
ln -sf ./commit-msg ../../../.git/hooks/commit-msg
ln -sf ./pre-push ../../../.git/hooks/pre-push
```

* Or you can copy these hooks to the `.git/hooks` directory directly.

> [!NOTE]
> If you want to bypass these hooks, you can run the `git commit`/`git
> push` command with `--no-verify` or `-n` option.

## Useful links

- [Git Hooks](https://git-scm.com/docs/githooks)
