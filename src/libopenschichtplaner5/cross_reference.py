from pathlib import Path
from typing import List, Dict, Any
from libopenschichtplaner5.registry import load_table, TABLE_NAMES


def load_all_tables(dbf_dir: Path) -> Dict[str, List[Any]]:
    """
    Loads all DBF tables from a given directory.

    :param dbf_dir: Path to the directory containing DBF files
    :return: Dictionary of table name to list of model instances
    """
    all_data = {***REMOVED***
    for table_name in TABLE_NAMES:
        expected_filename = f"{table_name***REMOVED***.DBF"
        dbf_path = dbf_dir / expected_filename

        if dbf_path.exists():
            try:
                table_data = load_table(table_name, dbf_path)
                all_data[table_name] = table_data
                print(f"✅ Loaded {table_name***REMOVED*** from {dbf_path***REMOVED***")
            except Exception as e:
                print(f"❌ Error loading {table_name***REMOVED***: {e***REMOVED***")
        else:
            print(f"❌ No DBF file found for table '{table_name***REMOVED***' in the directory.")
    return all_data


def match_entities(entity_data_1, entity_data_2, field_1: str, field_2: str):
    """
    Matches two entity lists based on matching field values.

    :param entity_data_1: First list of model instances
    :param entity_data_2: Second list of model instances
    :param field_1: Attribute name from first list
    :param field_2: Attribute name from second list
    :return: List of matched pairs
    """
    matched_pairs = []
    for e1 in entity_data_1:
        for e2 in entity_data_2:
            v1 = getattr(e1, field_1, None)
            v2 = getattr(e2, field_2, None)
            if v1 is not None and v2 is not None and v1 == v2:
                matched_pairs.append((e1, e2))
    return matched_pairs


def filter_notes_by_employee_id(notes_data, employee_id):
    """
    Filters notes by employee_id.

    :param notes_data: List of Note objects
    :param employee_id: Employee ID to filter by
    :return: Filtered list of Notes for the given employee_id
    """
    return [note for note in notes_data if note.employee_id == employee_id]


def filter_data_by_employee_id(matched_pairs, employee_id):
    """
    Filters matched data by employee_id.

    :param matched_pairs: List of matched pairs of entities
    :param employee_id: Employee ID to filter by
    :return: Filtered list of matched pairs for the given employee_id
    """
    filtered = []
    for pair in matched_pairs:
        if any(hasattr(entry, 'employee_id') and entry.employee_id == employee_id for entry in pair):
            filtered.append(pair)
    return filtered


def get_lookup_references():
    """
    Hardcodes the lookup references between tables and fields for matching.

    :return: A dictionary that maps tables to their matching fields in other tables
    """
    lookup = {
        "5NOTE": {
            "EMPLOYEEID": "5EMPL.ID",  # employee_id in 5NOTE matches ID in 5EMPL
            "SHIFTID": "5SHIFT.ID",  # shift_id in 5NOTE matches ID in 5SHIFT
        ***REMOVED***,
        "5EMPL": {
            "ID": "5NOTE.EMPLOYEEID",  # ID in 5EMPL matches employee_id in 5NOTE
            "ID": "5SPSHI.EMPLOYEEID",  # ID in 5EMPL matches employee_id in 5SPSHI
        ***REMOVED***,
        "5SHIFT": {
            "ID": "5SPSHI.SHIFTID",  # ID in 5SHIFT matches shift_id in 5SPSHI
            "ID": "5CYENT.SHIFTID",  # ID in 5SHIFT matches shift_id in 5CYENT
        ***REMOVED***,
        "5SPSHI": {
            "EMPLOYEEID": "5EMPL.ID",  # employee_id in 5SPSHI matches ID in 5EMPL
            "SHIFTID": "5SHIFT.ID",  # shift_id in 5SPSHI matches ID in 5SHIFT
        ***REMOVED***,
        "5CYENT": {
            "SHIFTID": "5SHIFT.ID",  # shift_id in 5CYENT matches ID in 5SHIFT
        ***REMOVED***,
        "5CYASS": {
            "EMPLOYEEID": "5EMPL.ID",  # employee_id in 5CYASS matches ID in 5EMPL
            "CYCLEID": "5CYCLE.ID",  # cycle_id in 5CYASS matches ID in 5CYCLE
        ***REMOVED***,
        "5GRACC": {
            "GROUPID": "5GROUP.ID",  # group_id in 5GRACC matches ID in 5GROUP
        ***REMOVED***,
        "5LEAEN": {
            "EMPLOYEEID": "5EMPL.ID",  # employee_id in 5LEAEN matches ID in 5EMPL
            "LEAVETYPID": "5LEAVT.ID",  # leave_type_id in 5LEAEN matches ID in 5LEAVT
        ***REMOVED***,
        "5LEAVT": {
            "ID": "5LEAEN.LEAVETYPID",  # ID in 5LEAVT matches leave_type_id in 5LEAEN
        ***REMOVED***,
    ***REMOVED***
    return lookup


def perform_cross_referencing(dbf_dir: Path, employee_id: int = None) -> Dict[str, List[Any]]:
    """
    Perform cross-referencing between all tables using the hardcoded lookup references.

    :param dbf_dir: Path to the DBF files
    :param employee_id: Optional employee_id filter
    :return: A dictionary of matched data from all tables
    """
    all_data = load_all_tables(dbf_dir)
    lookup = get_lookup_references()

    matched_data = {***REMOVED***

    # Loop through all tables and perform matching based on the hardcoded lookup
    for table_name, fields in lookup.items():
        for field, reference in fields.items():
            # Extract the reference table and field from the lookup (e.g., 5EMPL.id => 5NOTE.employee_id)
            reference_table, reference_field = reference.split(".")

            # Match the data based on the reference fields
            matched_pairs = match_entities(all_data[table_name], all_data[reference_table], field, reference_field)

            matched_data[f"{table_name***REMOVED***_{field***REMOVED***_to_{reference_table***REMOVED***"] = matched_pairs

    if employee_id:
        matched_data = {
            key: filter_data_by_employee_id(value, employee_id)
            if isinstance(value, list) and value and isinstance(value[0], tuple)
            else value
            for key, value in matched_data.items()
        ***REMOVED***

    return matched_data


def resolve_employee_names_in_notes(notes_data, employee_dict):
    """
    Resolves employee names in notes based on employee_id.

    :param notes_data: List of Note objects
    :param employee_dict: Dictionary mapping employee_id to employee_name
    :return: List of resolved Note objects with employee names
    """
    resolved_notes = []
    for note in notes_data:
        employee_name = employee_dict.get(note.employee_id, "Unknown Employee")
        note.employee_name = employee_name
        resolved_notes.append(note)
    return resolved_notes


def display_matches(cross_referenced_data):
    """
    Displays the cross-referenced data with resolved employee names.

    :param cross_referenced_data: Dictionary of matched data from all tables
    """
    print("\nCross-referenced data:")
    for key, matched_pairs in cross_referenced_data.items():
        if matched_pairs:
            print(f"\n{key***REMOVED***:")
            for pair in matched_pairs:
                if isinstance(pair[0], Note):
                    print(f" - Employee: {pair[0].employee_name***REMOVED*** | Note ID: {pair[0].id***REMOVED*** | {pair[0].text1***REMOVED***")
                else:
                    print(" - ", " | ".join(str(p) for p in pair))
