Thank you for your interest in contributing to the Cortex Data Framework! Your contributions are highly valued and help make the project better. Please read through this guide to understand how to contribute effectively.


## How Can I Contribute?

You can contribute in several ways:
- **Reporting Bugs**: Found an issue? Create a detailed bug report.
- **Feature Requests**: Suggest a new feature or improvement.
- **Code Contributions**: Fix bugs, add new features, or improve documentation.
- **Review Pull Requests**: Help maintainers by reviewing open pull requests.

## Code of Conduct

Please adhere to our Code of Conduct. Be respectful and collaborative to maintain a welcoming community.

## Getting Started

#### 1. Fork the Repository: Click the "Fork" button on GitHub to create your own copy of the repository.


#### 2. Clone the Fork

```bash
git clone https://github.com/your-username/cortex.git
cd cortex
```


#### 3. Install Dependencies

```bash
dotnet restore
```


#### 4. Build the Project

```bash
dotnet build
```

#### 5. Run Tests

```bash
dotnet test
```

## Reporting Issues

If you encounter a bug or have a feature request:

1. Check the issue tracker to see if it's already reported.
2. Create a new issue if it hasn't been reported.
    - Include a clear title and detailed description.
    - Provide steps to reproduce the issue, if applicable.
    - Attach screenshots or logs, if relevant.

## Submitting Code Changes

#### 1. Branching: Create a new branch for your work
```bash
git checkout -b feature/your-feature-name
```
#### 2. Write Clear Commit Messages
```bash
git commit -m "Add: Descriptive message for the change"
```
#### 3. Push Changes
```bash
git push origin feature/your-feature-name
```


## Reporting Issues
1. **Title and Description**: Use a meaningful title and provide a clear description of the changes.
2. **Link Issues**: Reference related issues in the pull request.
3. **Code Style**: Ensure your code adheres to the existing style and conventions.
4. **Tests**: Include or update tests for your changes.
5. **Review Process**: Be patient while maintainers review your pull request. Respond promptly to feedback.


## Development Workflow

1. Sync your fork regularly to stay updated
```bash
git fetch upstream
git rebase upstream/main
```

2. Follow the branching model to keep your changes isolated and easy to review.



## Writing Tests

Tests are essential to ensure the reliability of the Cortex Framework. Before submitting your changes:
- Add or modify tests in the `Cortex.Tests` project.
- Run the tests locally using:
```bash
dotnet test
```
- Confirm that all tests pass.


## License

By contributing, you agree that your contributions will be licensed under the same MIT License as the project.