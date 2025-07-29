# src/libopenschichtplaner5/api.py
"""
REST API wrapper for Schichtplaner5 data.
Provides a FastAPI-based web service for data access.
"""

from fastapi import FastAPI, HTTPException, Query, Path, Depends
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from pathlib import Path as FilePath
import json
import io

from .query_engine import QueryEngine
from .reports import ReportGenerator
from .export import DataExporter, ExportFormat
from .utils.validation import DataValidator
from .performance import performance_monitor, monitor_performance
from .exceptions import DataNotFoundError


# Pydantic models for API
class QueryFilter(BaseModel):
    """Query filter definition."""
    field: str
    operator: str = "="
    value: Any


class QueryRequest(BaseModel):
    """Query request body."""
    table: str
    filters: List[QueryFilter] = []
    joins: List[str] = []
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: Optional[str] = None
    order_desc: bool = False


class EmployeeResponse(BaseModel):
    """Employee response model."""
    id: int
    name: str
    firstname: str
    position: Optional[str]
    email: Optional[str]
    phone: Optional[str]
    empstart: Optional[date]
    empend: Optional[date]

    class Config:
        orm_mode = True


class AbsenceResponse(BaseModel):
    """Absence response model."""
    id: int
    employee_id: int
    date: date
    leave_type_id: int
    type: int

    class Config:
        orm_mode = True


class ExportRequest(BaseModel):
    """Export request body."""
    table: str
    format: str = Field(..., regex="^(csv|json|excel|html|markdown)$")
    filters: List[QueryFilter] = []
    fields: Optional[List[str]] = None


class ReportRequest(BaseModel):
    """Report request body."""
    report_type: str = Field(..., regex="^(absence|staffing|shifts|overtime)$")
    parameters: Dict[str, Any] = {}


class SchichtplanerAPI:
    """REST API for Schichtplaner5 data."""

    def __init__(self, dbf_dir: FilePath, title: str = "Schichtplaner5 API",
                 version: str = "1.0.0"):
        self.app = FastAPI(
            title=title,
            version=version,
            description="REST API for accessing Schichtplaner5 data"
        )

        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Initialize components
        self.dbf_dir = dbf_dir
        self.engine = None
        self.report_generator = None
        self.exporter = DataExporter()
        self.validator = DataValidator()

        # Setup routes
        self._setup_routes()

        # Startup event
        @self.app.on_event("startup")
        async def startup_event():
            self._initialize_engine()

    def _initialize_engine(self):
        """Initialize query engine."""
        try:
            self.engine = QueryEngine(self.dbf_dir)
            self.report_generator = ReportGenerator(self.engine)
        except Exception as e:
            print(f"Failed to initialize engine: {e}")
            raise

    def _setup_routes(self):
        """Setup all API routes."""

        # Health check
        @self.app.get("/health")
        async def health_check():
            return {
                "status": "healthy",
                "tables_loaded": len(self.engine.loaded_tables) if self.engine else 0,
                "timestamp": datetime.now().isoformat()
            }

        # Table info
        @self.app.get("/tables")
        async def list_tables():
            """List all available tables."""
            if not self.engine:
                raise HTTPException(status_code=503, detail="Engine not initialized")

            return {
                "tables": [
                    {
                        "name": name,
                        "records": len(records)
                    }
                    for name, records in self.engine.loaded_tables.items()
                ]
            }

        @self.app.get("/tables/{table_name}")
        async def get_table_info(table_name: str):
            """Get information about a specific table."""
            if table_name not in self.engine.loaded_tables:
                raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

            records = self.engine.loaded_tables[table_name]

            # Get sample record for field info
            fields = []
            if records:
                sample = records[0]
                for attr in dir(sample):
                    if not attr.startswith('_') and not callable(getattr(sample, attr)):
                        value = getattr(sample, attr)
                        fields.append({
                            "name": attr,
                            "type": type(value).__name__
                        })

            return {
                "name": table_name,
                "record_count": len(records),
                "fields": fields
            }

        # Query endpoint
        @self.app.post("/query")
        @monitor_performance("api_query")
        async def execute_query(request: QueryRequest):
            """Execute a query."""
            query = self.engine.query().select(request.table)

            # Apply filters
            for filter in request.filters:
                query = query.where(filter.field, filter.operator, filter.value)

            # Apply joins
            for join_table in request.joins:
                query = query.join(join_table)

            # Apply ordering
            if request.order_by:
                query = query.order_by(request.order_by, not request.order_desc)

            # Apply pagination
            if request.offset:
                query = query.offset(request.offset)
            if request.limit:
                query = query.limit(request.limit)

            # Execute
            result = query.execute()

            return {
                "success": True,
                "count": result.count,
                "data": result.to_dict(),
                "execution_time": result.execution_time
            }

        # Employee endpoints
        @self.app.get("/employees", response_model=List[EmployeeResponse])
        async def list_employees(
                limit: int = Query(100, ge=1, le=1000),
                offset: int = Query(0, ge=0),
                position: Optional[str] = None,
                active_only: bool = False
        ):
            """List employees with filtering."""
            query = self.engine.query().select("5EMPL")

            if position:
                query = query.where("position", "=", position)
            if active_only:
                query = query.where("empend", "is_null", None)

            query = query.offset(offset).limit(limit)
            result = query.execute()

            return result.to_dict()

        @self.app.get("/employees/{employee_id}")
        async def get_employee(employee_id: int = Path(..., ge=1)):
            """Get employee details."""
            try:
                profile = self.engine.get_employee_full_profile(employee_id)
                return profile
            except DataNotFoundError:
                raise HTTPException(status_code=404, detail=f"Employee {employee_id} not found")

        @self.app.get("/employees/{employee_id}/schedule")
        async def get_employee_schedule(
                employee_id: int = Path(..., ge=1),
                start_date: Optional[date] = None,
                end_date: Optional[date] = None
        ):
            """Get employee schedule."""
            schedule = self.engine.get_employee_schedule(employee_id, start_date, end_date)
            return {
                "employee_id": employee_id,
                "schedule": schedule
            }

        @self.app.get("/employees/{employee_id}/absences", response_model=List[AbsenceResponse])
        async def get_employee_absences(
                employee_id: int = Path(..., ge=1),
                year: Optional[int] = None
        ):
            """Get employee absences."""
            query = self.engine.query().select("5ABSEN").where("employee_id", "=", employee_id)

            if year:
                start_date = date(year, 1, 1)
                end_date = date(year, 12, 31)
                query = query.where_date_range("date", start_date, end_date)

            result = query.execute()
            return result.to_dict()

        # Group endpoints
        @self.app.get("/groups")
        async def list_groups():
            """List all groups."""
            result = self.engine.query().select("5GROUP").order_by("name").execute()
            return result.to_dict()

        @self.app.get("/groups/{group_id}/members")
        async def get_group_members(group_id: int = Path(..., ge=1)):
            """Get members of a group."""
            members = self.engine.get_group_members(group_id)
            return {
                "group_id": group_id,
                "members": members
            }

        # Export endpoint
        @self.app.post("/export")
        async def export_data(request: ExportRequest):
            """Export data in various formats."""
            # Build query
            query = self.engine.query().select(request.table)

            for filter in request.filters:
                query = query.where(filter.field, filter.operator, filter.value)

            result = query.execute()

            if not result.records:
                raise HTTPException(status_code=404, detail="No data to export")

            # Export
            data = result.to_dict()

            # Filter fields if specified
            if request.fields:
                data = [
                    {k: v for k, v in record.items() if k in request.fields}
                    for record in data
                ]

            # Export to bytes
            output = io.BytesIO()

            if request.format == "csv":
                content = self.exporter.to_csv(data)
                output.write(content.encode('utf-8'))
                media_type = "text/csv"
                filename = f"export_{request.table}.csv"
            elif request.format == "json":
                content = self.exporter.to_json(data)
                output.write(content.encode('utf-8'))
                media_type = "application/json"
                filename = f"export_{request.table}.json"
            elif request.format == "excel":
                content = self.exporter.to_excel(data)
                output.write(content)
                media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                filename = f"export_{request.table}.xlsx"
            else:
                content = self.exporter.to_html(data)
                output.write(content.encode('utf-8'))
                media_type = "text/html"
                filename = f"export_{request.table}.html"

            output.seek(0)

            return StreamingResponse(
                output,
                media_type=media_type,
                headers={
                    "Content-Disposition": f"attachment; filename={filename}"
                }
            )

        # Report endpoints
        @self.app.post("/reports")
        async def generate_report(request: ReportRequest):
            """Generate various reports."""
            try:
                if request.report_type == "absence":
                    report = self.report_generator.employee_absence_report(
                        request.parameters.get("employee_id"),
                        request.parameters.get("year", datetime.now().year)
                    )
                elif request.report_type == "staffing":
                    report = self.report_generator.group_staffing_report(
                        request.parameters.get("group_id"),
                        request.parameters.get("date")
                    )
                elif request.report_type == "shifts":
                    report = self.report_generator.shift_distribution_report(
                        request.parameters.get("start_date"),
                        request.parameters.get("end_date"),
                        request.parameters.get("group_id")
                    )
                elif request.report_type == "overtime":
                    report = self.report_generator.overtime_analysis_report(
                        request.parameters.get("employee_id"),
                        request.parameters.get("month"),
                        request.parameters.get("year")
                    )
                else:
                    raise ValueError(f"Unknown report type: {request.report_type}")

                return report.to_dict()

            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        # Validation endpoint
        @self.app.post("/validate")
        async def validate_data():
            """Validate data integrity."""
            report = self.validator.validate_all_tables(self.engine.loaded_tables)

            return {
                "valid": not report.has_errors(),
                "errors": len(report.errors),
                "warnings": len(report.warnings),
                "details": {
                    "errors": [str(e) for e in report.errors[:10]],
                    "warnings": [str(w) for w in report.warnings[:10]],
                    "statistics": report.statistics
                }
            }

        # Performance stats
        @self.app.get("/stats/performance")
        async def get_performance_stats():
            """Get performance statistics."""
            return performance_monitor.get_statistics()

        # Search endpoint
        @self.app.get("/search/employees")
        async def search_employees(
                q: str = Query(..., min_length=2),
                limit: int = Query(20, ge=1, le=100)
        ):
            """Search employees by name or other fields."""
            results = self.engine.search_employees(q)
            return results[:limit]


def create_api(dbf_dir: FilePath, **kwargs) -> FastAPI:
    """Create and return the FastAPI application."""
    api = SchichtplanerAPI(dbf_dir, **kwargs)
    return api.app


# Standalone runner
if __name__ == "__main__":
    import uvicorn
    import argparse

    parser = argparse.ArgumentParser(description="Run Schichtplaner5 API server")
    parser.add_argument("--dir", required=True, help="DBF directory path")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")

    args = parser.parse_args()

    app = create_api(FilePath(args.dir))

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        reload=args.reload
    )