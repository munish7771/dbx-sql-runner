class DbxRunnerError(Exception):
    """Base exception for dbx-sql-runner errors."""
    pass

class DbxConfigurationError(DbxRunnerError):
    """Raised when configuration is missing or invalid."""
    pass

class DbxAuthenticationError(DbxRunnerError):
    """Raised when authentication with Databricks fails."""
    pass


class DbxExecutionError(DbxRunnerError):
    """Raised when a SQL execution fails."""
    pass

class DbxDependencyError(DbxRunnerError):
    """Raised when there are issues with model dependencies (e.g. cycles)."""
    pass

class DbxModelLoadingError(DbxRunnerError):
    """Raised when loading models fails (e.g. directory not found)."""
    pass

class DbxTemplateError(DbxRunnerError):
    """Raised when SQL templating fails."""
    pass
