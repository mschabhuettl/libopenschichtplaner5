# ==============================================================================
# libopenschichtplaner5 — Build-/Test-Image (kein Runtime-Service)
#
# Die Library wird als pip-Paket konsumiert; dieses Image dient ausschließlich
# dazu, Lint + Testsuite reproduzierbar in einem Container laufen zu lassen:
#
#   docker compose run --rm test        # ruff + pytest
#   docker build --target test -t libopenschichtplaner5:test .
# ==============================================================================

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /lib

# Paket + Dev-/Postgres-Extras installieren (Metadaten zuerst für Layer-Cache)
COPY pyproject.toml README.md LICENSE ./
COPY sp5lib/ sp5lib/
RUN pip install --no-cache-dir -e ".[dev,postgres]"

# ── Stage test: ruff + pytest ─────────────────────────────────────────────────
FROM base AS test
COPY tests/ tests/
CMD ["sh", "-c", "ruff check . && pytest -v"]
