import json

# =========================================================
# PREPARE VALUES FOR BIGQUERY
# =========================================================

def _to_bq_string(value):
    """Converts a value into a BigQuery STRING format"""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, default=str)
    if isinstance(value, str):
        return value
    return str(value)


def normalize_for_bq(doc, repeated_fields):
    """
    Formats a document so it matches the BigQuery schema.

    - List fields are always saved as arrays.
    - Missing list fields become empty arrays.
    - Single values in list fields are wrapped into a list.
    - Other fields are converted into strings.
    """
    normalized = {}
    for key, value in doc.items():
        key_lower = key.lower()
        if key_lower in repeated_fields:
            if isinstance(value, list):
                items = value
            elif value is None:
                items = []
            else:
                items = [value]
            normalized[key] = [_to_bq_string(v) for v in items]
        else:
            normalized[key] = _to_bq_string(value)
    return normalized
