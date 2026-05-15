# Contributing to cluster-pulse

Thank you for your interest in contributing! This guide covers everything you need to get started.

## Prerequisites

- **JDK 17+**
- **sbt 1.x**

## Building

```bash
sbt compile
```

## Running Tests

```bash
sbt test
```

### Code Coverage

```bash
sbt coverage test coverageReport
```

Reports are generated in `target/scala-3.8.1/scoverage-report/`. The build enforces minimum 80% statement and 75% branch coverage.

## Code Style

This project uses [Scalafmt](https://scalameta.org/scalafmt/) for consistent formatting. Before submitting a PR:

```bash
sbt scalafmtCheckAll
```

To auto-format:

```bash
sbt scalafmtAll
```

## Submitting a Pull Request

1. Fork the repository and create a feature branch from `main`.
2. Make your changes, adding tests for new functionality.
3. Ensure all tests pass (`sbt test`) and formatting is correct (`sbt scalafmtCheckAll`).
4. Open a pull request against `main` with a clear description of the change.

## Reporting Issues

Open a GitHub issue with:
- A clear title and description.
- Steps to reproduce (if applicable).
- Expected vs. actual behavior.

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
