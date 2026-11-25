class DomainError(Exception):
    """Base class for domain rule violations."""


class InvalidDefinition(DomainError):
    """A saga definition breaks a structural rule."""


class InvalidTransition(DomainError):
    """An operation is not allowed in the saga's current state."""
