"""Visibility lifetimes, measured from the existing event freshness anchor."""
SEVERITY_TTL_MINUTES = {'light': 30, 'moderate': 60, 'severe': 180}

# Legacy rows without severity get the shortest lifetime; no 24h fallback.
TTL_SQL = (
    "CASE v.severity "
    + ' '.join(f"WHEN '{label}' THEN interval '{minutes} minutes'"
               for label, minutes in SEVERITY_TTL_MINUTES.items())
    + " ELSE interval '30 minutes' END"
)
