# Neue Datei: libopenschichtplaner5/src/libopenschichtplaner5/exceptions.py

class SchichtplanerError(Exception):
    """Base exception for all Schichtplaner errors."""
    pass

class DataNotFoundError(SchichtplanerError):
    """Raised when requested data is not found."""
    pass

class InvalidDateRangeError(SchichtplanerError):
    """Raised when date range is invalid."""
    pass

class RelationshipError(SchichtplanerError):
    """Raised when relationship resolution fails."""
    pass

class DBFLoadError(SchichtplanerError):
    """Raised when DBF file cannot be loaded."""
    pass