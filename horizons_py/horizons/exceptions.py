class HorizonsError(Exception):
    """Base SDK error."""

class NotFoundError(HorizonsError):
    pass

class AuthError(HorizonsError):
    pass

class ValidationError(HorizonsError):
    pass

class ServerError(HorizonsError):
    pass

class StreamError(HorizonsError):
    pass
