# src/libopenschichtplaner5/performance.py
"""
Performance monitoring and profiling utilities.
Tracks performance metrics and provides optimization insights.
"""

import time
import functools
import logging
import psutil
import sys
from typing import Dict, Any, List, Callable, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
from datetime import datetime
import threading
import json
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetric:
    """Single performance measurement."""
    operation: str
    start_time: float
    end_time: float
    memory_before: float
    memory_after: float
    record_count: int = 0
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> float:
        """Duration in seconds."""
        return self.end_time - self.start_time

    @property
    def memory_delta(self) -> float:
        """Memory change in MB."""
        return self.memory_after - self.memory_before

    @property
    def records_per_second(self) -> float:
        """Processing rate."""
        if self.duration > 0 and self.record_count > 0:
            return self.record_count / self.duration
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "operation": self.operation,
            "duration": round(self.duration, 3),
            "memory_delta_mb": round(self.memory_delta, 2),
            "record_count": self.record_count,
            "records_per_second": round(self.records_per_second, 1),
            "timestamp": datetime.fromtimestamp(self.start_time).isoformat(),
            "error": self.error,
            "metadata": self.metadata
        }


class PerformanceMonitor:
    """Global performance monitoring system."""

    def __init__(self, max_history: int = 1000):
        self.metrics: deque[PerformanceMetric] = deque(maxlen=max_history)
        self.operation_stats: Dict[str, List[float]] = defaultdict(list)
        self.active_operations: Dict[str, Tuple[float, float]] = {}
        self._lock = threading.Lock()
        self._process = psutil.Process()

    def start_operation(self, operation: str) -> str:
        """Start monitoring an operation."""
        with self._lock:
            start_time = time.time()
            memory_before = self._get_memory_usage()

            # Generate unique ID for nested operations
            op_id = f"{operation}_{id(threading.current_thread())}_{start_time}"
            self.active_operations[op_id] = (start_time, memory_before)

            logger.debug(f"Started monitoring: {operation}")
            return op_id

    def end_operation(self, op_id: str, record_count: int = 0,
                      error: Optional[str] = None, metadata: Dict[str, Any] = None) -> PerformanceMetric:
        """End monitoring an operation."""
        with self._lock:
            if op_id not in self.active_operations:
                raise ValueError(f"Unknown operation ID: {op_id}")

            start_time, memory_before = self.active_operations.pop(op_id)
            end_time = time.time()
            memory_after = self._get_memory_usage()

            # Extract operation name from ID
            operation = op_id.split('_')[0]

            metric = PerformanceMetric(
                operation=operation,
                start_time=start_time,
                end_time=end_time,
                memory_before=memory_before,
                memory_after=memory_after,
                record_count=record_count,
                error=error,
                metadata=metadata or {}
            )

            self.metrics.append(metric)
            self.operation_stats[operation].append(metric.duration)

            logger.debug(f"Completed {operation}: {metric.duration:.3f}s, {metric.memory_delta:.2f}MB")
            return metric

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        return self._process.memory_info().rss / 1024 / 1024

    def get_statistics(self, operation: Optional[str] = None) -> Dict[str, Any]:
        """Get performance statistics."""
        if operation:
            return self._get_operation_stats(operation)

        # Overall statistics
        stats = {
            "total_operations": len(self.metrics),
            "active_operations": len(self.active_operations),
            "memory_usage_mb": round(self._get_memory_usage(), 2),
            "operations": {}
        }

        # Per-operation statistics
        for op_name, durations in self.operation_stats.items():
            if durations:
                stats["operations"][op_name] = {
                    "count": len(durations),
                    "avg_duration": round(sum(durations) / len(durations), 3),
                    "min_duration": round(min(durations), 3),
                    "max_duration": round(max(durations), 3),
                    "total_duration": round(sum(durations), 3)
                }

        return stats

    def _get_operation_stats(self, operation: str) -> Dict[str, Any]:
        """Get statistics for specific operation."""
        metrics = [m for m in self.metrics if m.operation == operation]

        if not metrics:
            return {"error": f"No metrics found for operation: {operation}"}

        durations = [m.duration for m in metrics]
        memory_deltas = [m.memory_delta for m in metrics]
        rates = [m.records_per_second for m in metrics if m.records_per_second > 0]

        return {
            "operation": operation,
            "count": len(metrics),
            "duration": {
                "avg": round(sum(durations) / len(durations), 3),
                "min": round(min(durations), 3),
                "max": round(max(durations), 3),
                "total": round(sum(durations), 3)
            },
            "memory_mb": {
                "avg_delta": round(sum(memory_deltas) / len(memory_deltas), 2),
                "max_delta": round(max(memory_deltas), 2)
            },
            "throughput": {
                "avg_records_per_sec": round(sum(rates) / len(rates), 1) if rates else 0,
                "max_records_per_sec": round(max(rates), 1) if rates else 0
            },
            "errors": sum(1 for m in metrics if m.error is not None)
        }

    def get_recent_metrics(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent performance metrics."""
        recent = list(self.metrics)[-limit:]
        return [m.to_dict() for m in reversed(recent)]

    def export_metrics(self, filepath: Path):
        """Export metrics to JSON file."""
        data = {
            "timestamp": datetime.now().isoformat(),
            "statistics": self.get_statistics(),
            "recent_metrics": self.get_recent_metrics(100)
        }

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

    def clear(self):
        """Clear all metrics."""
        with self._lock:
            self.metrics.clear()
            self.operation_stats.clear()
            self.active_operations.clear()


# Global monitor instance
performance_monitor = PerformanceMonitor()


def monitor_performance(operation_name: Optional[str] = None):
    """Decorator to monitor function performance."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Use function name if operation name not provided
            op_name = operation_name or f"{func.__module__}.{func.__name__}"

            op_id = performance_monitor.start_operation(op_name)
            error = None
            record_count = 0

            try:
                result = func(*args, **kwargs)

                # Try to extract record count from result
                if hasattr(result, 'count'):
                    record_count = result.count
                elif hasattr(result, '__len__'):
                    record_count = len(result)

                return result

            except Exception as e:
                error = str(e)
                raise

            finally:
                performance_monitor.end_operation(
                    op_id,
                    record_count=record_count,
                    error=error
                )

        return wrapper

    return decorator


class PerformanceOptimizer:
    """Analyzes performance metrics and suggests optimizations."""

    def __init__(self, monitor: PerformanceMonitor):
        self.monitor = monitor

    def analyze(self) -> Dict[str, Any]:
        """Analyze performance and provide recommendations."""
        stats = self.monitor.get_statistics()
        recommendations = []

        # Check for slow operations
        for op_name, op_stats in stats.get("operations", {}).items():
            avg_duration = op_stats.get("avg_duration", 0)

            if avg_duration > 5.0:
                recommendations.append({
                    "operation": op_name,
                    "issue": "Slow operation",
                    "avg_duration": avg_duration,
                    "recommendation": "Consider using streaming or batch processing"
                })

        # Check memory usage
        memory_usage = stats.get("memory_usage_mb", 0)
        if memory_usage > 1000:  # 1GB
            recommendations.append({
                "issue": "High memory usage",
                "memory_mb": memory_usage,
                "recommendation": "Enable streaming mode or reduce cache size"
            })

        # Check for operations with high error rates
        for op_name, op_stats in stats.get("operations", {}).items():
            total = op_stats.get("count", 0)
            errors = sum(1 for m in self.monitor.metrics
                         if m.operation == op_name and m.error)

            if total > 0 and errors / total > 0.1:  # >10% error rate
                recommendations.append({
                    "operation": op_name,
                    "issue": "High error rate",
                    "error_rate": round(errors / total * 100, 1),
                    "recommendation": "Review error logs and add better error handling"
                })

        return {
            "analysis_time": datetime.now().isoformat(),
            "total_operations": stats.get("total_operations", 0),
            "memory_usage_mb": memory_usage,
            "recommendations": recommendations,
            "top_slow_operations": self._get_slowest_operations(5),
            "memory_intensive_operations": self._get_memory_intensive_operations(5)
        }

    def _get_slowest_operations(self, limit: int) -> List[Dict[str, Any]]:
        """Get slowest operations."""
        operations = []

        for op_name, durations in self.monitor.operation_stats.items():
            if durations:
                operations.append({
                    "operation": op_name,
                    "avg_duration": round(sum(durations) / len(durations), 3),
                    "max_duration": round(max(durations), 3),
                    "count": len(durations)
                })

        # Sort by average duration
        operations.sort(key=lambda x: x["avg_duration"], reverse=True)
        return operations[:limit]

    def _get_memory_intensive_operations(self, limit: int) -> List[Dict[str, Any]]:
        """Get most memory-intensive operations."""
        memory_by_op = defaultdict(list)

        for metric in self.monitor.metrics:
            if metric.memory_delta > 0:
                memory_by_op[metric.operation].append(metric.memory_delta)

        operations = []
        for op_name, deltas in memory_by_op.items():
            if deltas:
                operations.append({
                    "operation": op_name,
                    "avg_memory_mb": round(sum(deltas) / len(deltas), 2),
                    "max_memory_mb": round(max(deltas), 2),
                    "count": len(deltas)
                })

        # Sort by average memory usage
        operations.sort(key=lambda x: x["avg_memory_mb"], reverse=True)
        return operations[:limit]


# Example usage
@monitor_performance("load_employee_table")
def load_employees_with_monitoring(path: Path) -> List[Any]:
    """Example function with performance monitoring."""
    from .models.employee import load_employees
    return load_employees(path)


def benchmark_operation(func: Callable, iterations: int = 5) -> Dict[str, Any]:
    """Benchmark a function over multiple iterations."""
    durations = []
    memory_usage = []

    for i in range(iterations):
        start_mem = performance_monitor._get_memory_usage()
        start_time = time.time()

        try:
            func()
        except Exception as e:
            logger.error(f"Benchmark iteration {i} failed: {e}")
            continue

        duration = time.time() - start_time
        end_mem = performance_monitor._get_memory_usage()

        durations.append(duration)
        memory_usage.append(end_mem - start_mem)

        # Cool down between iterations
        time.sleep(0.5)

    if not durations:
        return {"error": "All benchmark iterations failed"}

    return {
        "iterations": len(durations),
        "duration": {
            "avg": round(sum(durations) / len(durations), 3),
            "min": round(min(durations), 3),
            "max": round(max(durations), 3),
            "std_dev": round(_std_dev(durations), 3)
        },
        "memory_mb": {
            "avg": round(sum(memory_usage) / len(memory_usage), 2),
            "max": round(max(memory_usage), 2)
        }
    }


def _std_dev(values: List[float]) -> float:
    """Calculate standard deviation."""
    if len(values) < 2:
        return 0.0
    avg = sum(values) / len(values)
    variance = sum((x - avg) ** 2 for x in values) / (len(values) - 1)
    return variance ** 0.5