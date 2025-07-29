# src/libopenschichtplaner5/relationships.py
"""
Relationship management - now uses the improved implementation.
This file provides compatibility with existing code.
"""

# Import everything from the improved version
from .relationships_improved import (
    RelationType,
    RelationshipSchema,
    FieldMapping,
    RelationshipResolver,
    RelationshipIndex,
    improved_relationship_manager,
    relationship_manager,
    get_entity_with_relations,
    create_default_resolver,
)

# Re-export the improved relationship manager as the default
# This maintains compatibility with existing code

# Additional compatibility exports
TableRelationship = RelationshipSchema  # Alias for backward compatibility

# Ensure relationships are properly initialized
if len(improved_relationship_manager.schemas) == 0:
    # Re-create with default relationships
    improved_relationship_manager = create_default_resolver()