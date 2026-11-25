"""Fails if the domain imports anything but an allow-list of pure stdlib modules
or any outer layer (application, adapters, ports)."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "src" / "cortex9_saga"
ALLOWED_STDLIB = {
    "__future__",
    "collections",
    "dataclasses",
    "enum",
    "types",
    "typing",
}


def imported_modules(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.append(node.module)
    return out


def main() -> int:
    errors: list[str] = []
    for f in sorted((ROOT / "domain").glob("*.py")):
        for mod in imported_modules(f):
            top = mod.split(".")[0]
            if mod.startswith("cortex9_saga."):
                if not mod.startswith("cortex9_saga.domain"):
                    errors.append(f"{f.name}: domain imports outer layer {mod}")
            elif top not in ALLOWED_STDLIB:
                errors.append(f"{f.name}: domain imports non-pure module {mod}")
    for f in [ROOT / "ports.py", *sorted((ROOT / "application").glob("*.py"))]:
        for mod in imported_modules(f):
            if mod.startswith("cortex9_saga.adapters"):
                errors.append(f"{f.relative_to(ROOT)}: depends on adapters ({mod})")
    for e in errors:
        print(f"architecture violation: {e}", file=sys.stderr)
    if not errors:
        print("architecture OK")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
