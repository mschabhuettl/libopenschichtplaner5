"""Resolution of host-application resource directories.

Historically these modules lived inside the OpenSchichtplaner5 ``backend/`` tree and
located sibling resources (``backend/data``, ``backend/api/data``, the Alembic
directory) via paths relative to ``__file__``. Once this library is installed as a
standalone package (``libopenschichtplaner5``) that assumption no longer holds — the
package lives in ``site-packages`` and the host app's data lives elsewhere.

The host application therefore tells the library where its backend root is via the
``SP5_BACKEND_DIR`` environment variable. When unset, we fall back to the legacy
``<this_package_parent>`` location so an in-tree checkout keeps working unchanged.
"""

from __future__ import annotations

import os


def backend_dir() -> str:
    """Return the host application's backend root directory.

    Honors ``SP5_BACKEND_DIR``; otherwise falls back to the directory that contains
    this package (the legacy in-tree layout ``<backend>/sp5lib/``).
    """
    env = os.environ.get("SP5_BACKEND_DIR")
    if env:
        return os.path.abspath(env)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def data_dir() -> str:
    """``<backend>/data`` — JSON-backed settings (changelog, swaps, comments …)."""
    path = os.path.join(backend_dir(), "data")
    os.makedirs(path, exist_ok=True)
    return path


def api_data_dir() -> str:
    """``<backend>/api/data`` — availability / skills JSON written by the API layer."""
    return os.path.join(backend_dir(), "api", "data")
