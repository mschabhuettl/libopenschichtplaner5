def normalize_string(value: object) -> str:
    """
    Normalize string from DBF record:
    - decode bytes if needed
    - remove null bytes
    - strip whitespace
    - fix encoding issues
    """
    # Kyrillische Zeichen die eigentlich Umlaute sein sollten
    CHAR_REPLACEMENTS = {
        "ь": "ü",
        "д": "ä",
        "ц": "ö",
        "Ь": "Ü",
        "Д": "Ä",
        "Ц": "Ö",
        "Я": "ß",
        "ќ": "ü",
        "Ђ": "Ä",
    }

    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("cp1252", errors="ignore")

    value = str(value).replace("\x00", "").strip()

    # Ersetze bekannte Problemzeichen
    for old, new in CHAR_REPLACEMENTS.items():
        value = value.replace(old, new)

    return value