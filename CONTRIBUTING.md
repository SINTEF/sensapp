# Contributing

Thank you for considering contributing to SensApp! We welcome contributions, though it has never happened yet.

## Discuss Changes

For significant changes, please open an issue first to discuss what you would like to change. This helps us align your contributions with our project goals.

## Pre-commits hooks

Please use pre-commits hooks to check your changes locally. Your change will also be checked by the continuous integration (CI) pipeline, but it is faster to catch errors locally before pushing your changes.

```
pip install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg
```

## Language

We use Rust.

## Code Style

We follow the [Rust Style Guide](https://github.com/rust-lang/rust/tree/HEAD/src/doc/style-guide/src).

## Tests

Update tests as appropriate. New features should come with additional tests.

## Documentation

 **Documentation**: Update the `README.md` or other documentation with details of changes to the interface or additional features.

## Issues

Make sure you use the latest version of SensApp and please include enough information about the issue. The more information you provide, the easier it is to reproduce the issue and to fix it.

## ~~Pull~~ Merge Requests

You are not allowed to push to the `main` branch. Please create a merge request instead.

Please do NOT `squash` your merge requests. You are welcome to organise and clean your commits first, but we want to keep the commit history.

Avoid merging your own pull requests without a review. Do not merge a pull request with failing tests.

## Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/).

```
<type>[(optional scope)]: <description>

[optional body]

[optional footer(s)]
```

Examples:

```
fix: prevent infinite loop when it rains
docs(architecture): correct spelling of banana
```

Valid types are: fix, feat, chore, docs, style, refactor, perf, test, revert, ci, and build.

To please the [gitmoji](https://gitmoji.dev) enthousiasts, unicode emojis are allowed but not enforced. Commit messages with emojis still must respect the conventional commit format.

Examples:

```
fix: 🐛 prevent infinite loop when it rains
docs(architecture): 📝 correct spelling of banana
```
