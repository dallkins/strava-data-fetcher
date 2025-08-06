# Contributing to Strava Data Fetcher

Thank you for your interest in contributing to the Strava Data Fetcher project! This document provides guidelines and information for contributors.

## 🤝 How to Contribute

### Reporting Issues

1. **Check existing issues** first to avoid duplicates
2. **Use the issue template** when creating new issues
3. **Provide detailed information** including:
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (Python version, OS, etc.)
   - Relevant logs or error messages

### Suggesting Features

1. **Open a feature request** issue
2. **Describe the use case** and why it would be valuable
3. **Provide implementation ideas** if you have them
4. **Consider backward compatibility** implications

### Code Contributions

1. **Fork the repository**
2. **Create a feature branch** from `main`
3. **Make your changes** following our coding standards
4. **Add tests** for new functionality
5. **Update documentation** as needed
6. **Submit a pull request**

## 🏗 Development Setup

### Prerequisites

- Python 3.8+
- MySQL/MariaDB
- Git
- Virtual environment tool (venv, conda, etc.)

### Local Development

1. **Clone your fork**
   ```bash
   git clone https://github.com/yourusername/strava-data-fetcher.git
   cd strava-data-fetcher
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

4. **Set up environment**
   ```bash
   cp .env.example .env
   # Edit .env with your test configuration
   ```

5. **Run tests**
   ```bash
   pytest
   ```

## 📝 Coding Standards

### Python Style

We follow [PEP 8](https://pep8.org/) with some modifications:

- **Line length**: 100 characters (not 79)
- **String quotes**: Use double quotes for strings
- **Import order**: Use `isort` for consistent import ordering
- **Code formatting**: Use `black` for automatic formatting

### Code Quality Tools

Run these tools before submitting:

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Lint code
flake8 src/ tests/

# Type checking
mypy src/

# Run all quality checks
make lint  # If Makefile is available
```

### Documentation

- **Docstrings**: Use Google-style docstrings for all public functions/classes
- **Type hints**: Add type hints for all function parameters and return values
- **Comments**: Write clear, concise comments for complex logic
- **README**: Update README.md for user-facing changes

### Example Code Style

```python
"""
Module docstring describing the purpose.
"""

from typing import List, Optional, Dict, Any
import asyncio

from .utils.logging_config import get_logger
from .utils.error_handling import APIError

logger = get_logger(__name__)


class ExampleClass:
    """
    Example class demonstrating coding standards.
    
    This class shows how to structure code according to our guidelines
    with proper documentation, type hints, and error handling.
    """
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the example class.
        
        Args:
            config: Configuration dictionary with required settings
            
        Raises:
            ValueError: If configuration is invalid
        """
        self.config = config
        self.logger = get_logger(__name__)
    
    async def process_data(
        self, 
        data: List[Dict[str, Any]], 
        limit: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Process data with optional limit.
        
        Args:
            data: List of data dictionaries to process
            limit: Maximum number of items to process
            
        Returns:
            Dictionary with processing results
            
        Raises:
            APIError: If processing fails
        """
        try:
            # Implementation here
            results = {}
            
            for item in data[:limit] if limit else data:
                # Process each item
                pass
            
            self.logger.info(f"Processed {len(results)} items")
            return results
            
        except Exception as e:
            raise APIError(f"Processing failed: {e}")
```

## 🧪 Testing Guidelines

### Test Structure

- **Unit tests**: Test individual components in isolation
- **Integration tests**: Test component interactions
- **Test coverage**: Aim for >90% coverage for new code
- **Test naming**: Use descriptive test names that explain what is being tested

### Writing Tests

```python
import pytest
from unittest.mock import Mock, patch

from src.example_module import ExampleClass


class TestExampleClass:
    """Test suite for ExampleClass."""
    
    def test_initialization_success(self):
        """Test successful initialization with valid config."""
        config = {"key": "value"}
        instance = ExampleClass(config)
        assert instance.config == config
    
    def test_initialization_failure(self):
        """Test initialization failure with invalid config."""
        with pytest.raises(ValueError):
            ExampleClass({})
    
    @pytest.mark.asyncio
    async def test_process_data_success(self):
        """Test successful data processing."""
        config = {"key": "value"}
        instance = ExampleClass(config)
        
        data = [{"id": 1}, {"id": 2}]
        result = await instance.process_data(data)
        
        assert isinstance(result, dict)
        assert len(result) == 2
    
    @pytest.mark.asyncio
    async def test_process_data_with_limit(self):
        """Test data processing with limit parameter."""
        config = {"key": "value"}
        instance = ExampleClass(config)
        
        data = [{"id": i} for i in range(10)]
        result = await instance.process_data(data, limit=5)
        
        assert len(result) == 5
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_example.py

# Run with coverage
pytest --cov=src --cov-report=html

# Run only fast tests
pytest -m "not slow"

# Run with verbose output
pytest -v
```

## 📋 Pull Request Process

### Before Submitting

1. **Ensure tests pass**
   ```bash
   pytest
   ```

2. **Check code quality**
   ```bash
   black src/ tests/
   isort src/ tests/
   flake8 src/ tests/
   mypy src/
   ```

3. **Update documentation**
   - Add docstrings for new functions/classes
   - Update README.md if needed
   - Add/update type hints

4. **Write/update tests**
   - Add unit tests for new functionality
   - Add integration tests for new features
   - Ensure good test coverage

### Pull Request Template

When submitting a PR, include:

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests pass
- [ ] Code coverage maintained/improved

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No breaking changes (or clearly documented)
```

### Review Process

1. **Automated checks** must pass (CI/CD pipeline)
2. **Code review** by maintainers
3. **Testing** in development environment
4. **Approval** and merge by maintainers

## 🏷 Versioning

We use [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

## 🌟 Recognition

Contributors will be:
- Listed in the project's contributors section
- Mentioned in release notes for significant contributions
- Invited to join the maintainers team for ongoing contributors

## 📞 Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Email**: Contact maintainers directly for sensitive issues

## 📜 Code of Conduct

### Our Pledge

We pledge to make participation in our project a harassment-free experience for everyone, regardless of age, body size, disability, ethnicity, gender identity and expression, level of experience, nationality, personal appearance, race, religion, or sexual identity and orientation.

### Our Standards

Examples of behavior that contributes to creating a positive environment include:

- Using welcoming and inclusive language
- Being respectful of differing viewpoints and experiences
- Gracefully accepting constructive criticism
- Focusing on what is best for the community
- Showing empathy towards other community members

### Unacceptable Behavior

Examples of unacceptable behavior include:

- The use of sexualized language or imagery
- Trolling, insulting/derogatory comments, and personal or political attacks
- Public or private harassment
- Publishing others' private information without explicit permission
- Other conduct which could reasonably be considered inappropriate in a professional setting

### Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be reported by contacting the project maintainers. All complaints will be reviewed and investigated promptly and fairly.

## 🙏 Thank You

Thank you for contributing to the Strava Data Fetcher project! Your contributions help make this tool better for the entire community.

---

**Questions?** Feel free to open an issue or reach out to the maintainers.