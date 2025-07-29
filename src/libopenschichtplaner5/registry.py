# src/libopenschichtplaner5/registry.py
"""
Registry module - now uses the improved implementation.
This file provides compatibility with existing code.
"""

# Import everything from the improved version
from .registry_improved import (
    enhanced_registry,
    load_table,
    get_table_info,
    TABLE_NAMES,
    TABLE_METADATA,
    TableDefinition,
    PluginRegistry,
    register_standard_tables,
    register_table_definitions_from_json,
)

# For backward compatibility
def get_all_table_names():
    """Get all registered table names."""
    return TABLE_NAMES

def is_table_optional(name: str) -> bool:
    """Check if a table is optional."""
    info = get_table_info(name)
    return info.optional if info else False

# Ensure standard tables are registered
if not enhanced_registry.plugins:
    register_standard_tables()

# Export the global registry instance
registry = enhanced_registry