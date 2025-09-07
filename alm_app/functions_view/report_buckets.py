import re

def bucket_column_name(bucket) -> str:
    """
    Build a legal SQL column name, zero-padding the bucket number so
    alphabetical order == numeric order.

    Example for bucket_number=3,  start=2025-01-01, end=2025-03-31
        bucket_003_20250101_20250331
    """
    raw = (
        f"bucket_{bucket.bucket_number:03d}_"      # ← zero-pad to 3 digits
        f"{bucket.start_date:%Y%m%d}_{bucket.end_date:%Y%m%d}"
    )
    # Keep only letters, numbers, underscores; truncate to 63 chars (Postgres limit)
    return re.sub(r"[^0-9A-Za-z_]", "_", raw)[:63]
