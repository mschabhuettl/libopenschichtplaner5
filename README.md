# libopenschichtplaner5

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

The core library of OpenSchichtplaner5 - a comprehensive Python library for reading, analyzing, and processing **Schichtplaner5** database files (DBF format). This library provides the foundation for all other OpenSchichtplaner5 components.

## 🎯 Overview

libopenschichtplaner5 is designed to handle the complex task of reading and interpreting Schichtplaner5 shift planning data from DBF files. It provides type-safe data models, a powerful query engine, comprehensive validation, and relationship management for shift planning systems.

## 🏗️ Architecture

The library follows a modular design with clear separation of concerns:

```
libopenschichtplaner5/
├── db/              # DBF file reading and low-level data access
├── models/          # Type-safe data models for all 30+ table types
├── exceptions.py    # Custom exception hierarchy
├── export.py        # Multi-format data export capabilities
├── performance.py   # Performance monitoring and metrics
├── query_engine.py  # Fluent query interface
├── registry.py      # Plugin-based table registration system
├── relationships.py # Automatic foreign key resolution
├── reports.py       # Report generation utilities
├── streaming.py     # Memory-efficient streaming for large datasets
└── utils/           # Validation, logging, and utility functions
```

## 📦 Core Components

### 1. Registry System (`registry.py`, `registry_improved.py`)
A sophisticated plugin architecture that manages table definitions and loading:

```python
from libopenschichtplaner5 import enhanced_registry

# Load all tables with dependency resolution
tables = enhanced_registry.load_all_tables(Path("/path/to/dbf/files"))

# Access specific table data
employees = enhanced_registry.get_table("5EMPL")
```

**Features:**
- Automatic dependency resolution using topological sorting
- Plugin-based architecture for extensibility
- Dynamic loading with error handling
- Validation and metadata tracking

### 2. Data Models (`models/`)
Type-safe dataclasses for all Schichtplaner5 entities:

```python
from libopenschichtplaner5.models.employee import Employee
from libopenschichtplaner5.models.shift import Shift
from libopenschichtplaner5.models.absence import Absence

# Type-safe data access
employee = Employee(
    id=1,
    name="Mustermann",
    firstname="Max",
    shortname="MM",
    function="Assistant"
)
```

**Supported Models:**
- **Employee Management**: Employee, Group, GroupAssignment
- **Shift Planning**: Shift, ShiftDetail, EmployeeShift, WorkLocation
- **Leave Management**: Absence, LeaveType, LeaveEntitlement
- **System Management**: User, UserSetting, Build
- **Advanced Features**: Cycle, Holiday, Note, Overtime

### 3. Query Engine (`query_engine.py`)
Fluent interface for building complex data queries:

```python
from libopenschichtplaner5 import QueryEngine

engine = QueryEngine("/path/to/dbf/files")

# Build complex queries with method chaining
results = (engine
    .employees()
    .filter("department", "=", "Human Resources")
    .join("shifts") 
    .where("date", "between", ["2025-01-01", "2025-01-31"])
    .order_by("lastname")
    .limit(50)
    .execute())
```

**Query Features:**
- Method chaining for readable queries
- Automatic relationship joins
- Filtering with multiple operators
- Sorting and pagination
- Aggregation functions

### 4. Export System (`export.py`)
Multi-format data export with streaming support:

```python
from libopenschichtplaner5.export import DataExporter, ExportFormat

exporter = DataExporter()

# Export to various formats
exporter.export_table(employees, ExportFormat.CSV, "employees.csv")
exporter.export_table(shifts, ExportFormat.EXCEL, "shifts.xlsx") 
exporter.export_table(absences, ExportFormat.JSON, "absences.json")
```

**Supported Formats:**
- **CSV**: Comma-separated values
- **JSON**: Structured JSON data
- **Excel**: XLSX format with formatting
- **HTML**: Web-ready tables
- **Markdown**: Documentation-friendly format

## 🚀 Quick Start

### Basic Usage

```python
from pathlib import Path
from libopenschichtplaner5 import enhanced_registry

# Load all Schichtplaner5 tables
dbf_directory = Path("/path/to/your/dbf/files")
tables = enhanced_registry.load_all_tables(dbf_directory)

# Access specific tables
employees = enhanced_registry.get_table("5EMPL")
shifts = enhanced_registry.get_table("5SHIFT")
absences = enhanced_registry.get_table("5ABSEN")

print(f"Loaded {len(employees)} employees")
print(f"Loaded {len(shifts)} shift definitions")
print(f"Loaded {len(absences)} absence records")
```

## 📊 Supported Database Tables

libopenschichtplaner5 supports all 30 Schichtplaner5 database tables:

### Core Tables (Always Required)
- **5EMPL**: Employee master data
- **5GROUP**: Organizational groups
- **5SHIFT**: Shift type definitions
- **5WOPL**: Work locations/stations

### Assignment Tables
- **5GRASG**: Employee-group assignments
- **5SPSHI**: Current shift assignments
- **5MASHI**: Historical shift records

### Leave Management
- **5ABSEN**: Employee absences
- **5LEAVT**: Leave type definitions
- **5LEAEN**: Leave entitlements

See [DBF Tables Reference](../DBF_TABLES_REFERENCE.md) for complete table documentation.

## 🛠️ Installation

As part of the OpenSchichtplaner5 project:

```bash
# Install from the main project
cd openschichtplaner5
pip install -r requirements.txt

# The library will be available as:
import libopenschichtplaner5
```

## 📄 License

This library is part of the OpenSchichtplaner5 project and is licensed under the MIT License.
