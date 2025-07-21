#!/usr/bin/env python3
"""Analysiert DBF-Dateien"""
from pathlib import Path
import sys

# Add library to path
sys.path.insert(0, str(Path(__file__).parent / "libopenschichtplaner5" / "src"))

from libopenschichtplaner5.utils.analyzers import SchichtplanerAnalyzer

if __name__ == "__main__":
    dbf_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "./dbf_files")
    analyzer = SchichtplanerAnalyzer(dbf_dir)
    analyzer.analyze_all()
