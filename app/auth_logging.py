"""
Authentication event logging helpers.

Provides utilities for structured logging of authentication events
(login, logout, rejection) with consistent formatting for New Relic.
"""

from flask import request


def get_client_ip():
    """
    Get the client IP address from the request.

    Checks X-Forwarded-For header first (for proxied requests),
    falls back to remote_addr.

    Returns:
        str: Client IP address
    """
    return request.headers.get("X-Forwarded-For", request.remote_addr)
