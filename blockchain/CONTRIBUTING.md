# Contributing to Kumquat

Thank you for your interest in contributing to Kumquat! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).

## How Can I Contribute?

### Reporting Bugs

Before creating a bug report:

1. Check the [issue tracker](https://github.com/kumquat/kumquat/issues) to see if the problem has already been reported
2. If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/kumquat/kumquat/issues/new)

When creating a bug report, include as much information as possible:

- A clear and descriptive title
- Steps to reproduce the issue
- Expected behavior vs. actual behavior
- Screenshots or logs if applicable
- Your environment (OS, Rust version, etc.)

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues:

1. Check if the enhancement has already been suggested
2. [Create a new issue](https://github.com/kumquat/kumquat/issues/new) with a clear description of the enhancement
3. Provide specific examples of how the enhancement would work
4. Explain why this enhancement would be useful to most Kumquat users

### Pull Requests

1. Fork the repository
2. Create a new branch for your feature or bugfix
3. Make your changes
4. Run tests to ensure your changes don't break existing functionality
5. Submit a pull request

#### Pull Request Guidelines

- Follow the [Rust style guide](https://github.com/rust-lang/style-team/blob/master/guide/guide.md)
- Write or update tests for the changes you make
- Update documentation as needed
- Keep your PR focused on a single topic
- Reference any relevant issues
- Make sure all tests pass before submitting

## Development Workflow

### Setting Up Your Development Environment

1. Install Rust and Cargo
2. Clone the repository
3. Install development dependencies
4. Run `cargo build` to build the project

### Running Tests

```bash
# Run all tests
cargo test

# Run specific tests
cargo test <test_name>

# Run tests with verbose output
cargo test -- --nocapture
```

### Code Style

- Use `rustfmt` to format your code
- Use `clippy` to catch common mistakes
- Follow the project's coding standards (see [Code Standards](./docs/code_standards.md))

### File Size Limit

Kumquat enforces a strict 1000-line limit for all Rust files. This is checked by a pre-commit hook. See [File Size Policy](./docs/file_size_policy.md) for more details.

## Git Workflow

1. Create a branch from `main` for your work
2. Make your changes in small, logical commits
3. Push your branch to your fork
4. Submit a pull request to the `main` branch

### Commit Messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/) format:

```
type(scope): description

[optional body]

[optional footer]
```

Types include:
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code changes that neither fix bugs nor add features
- `test`: Adding or updating tests
- `chore`: Changes to the build process or auxiliary tools

## Documentation

- Document all public APIs
- Update the README.md if necessary
- Add examples for new features
- Keep documentation up-to-date with code changes

## Questions?

If you have any questions about contributing, please join our [Discord community](https://discord.gg/kumquat) or email us at team@kumquat.io.

Thank you for contributing to Kumquat!
