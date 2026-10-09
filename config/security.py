HSTS_MAX_AGE_SECONDS = 60 * 60 * 24 * 365
HSTS_HEADER = f"max-age={HSTS_MAX_AGE_SECONDS}; includeSubDomains; preload"

TALISMAN_BASELINE_OPTIONS = {
    "force_https": False,
    "strict_transport_security_max_age": HSTS_MAX_AGE_SECONDS,
    "strict_transport_security_preload": True,
}
