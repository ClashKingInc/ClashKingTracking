def sentry_filter(event, hint):
    """Filter out events that are not errors."""
    if "exception" in hint:
        exc_type, exc_value, exc_tb = hint["exception"]
        if exc_type == KeyboardInterrupt:
            return None
    return event
