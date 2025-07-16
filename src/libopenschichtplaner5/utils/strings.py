def normalize_string(value: object) -> str:
    """
    Normalize string from DBF record:
    - decode bytes if needed
    - remove null bytes
    - strip whitespace
    """
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("cp1252", errors="ignore")
    return str(value).replace("\x00", "").strip()
