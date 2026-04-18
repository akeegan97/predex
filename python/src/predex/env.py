from __future__ import annotations

import ast
import os
import re
from pathlib import Path

_ASSIGNMENT_RE = re.compile(
    r"^\s*(?:export\s+)?(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<value>.*)\s*$"
)


def _parse_dotenv_value(raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        return ""

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        try:
            parsed = ast.literal_eval(value)
        except (SyntaxError, ValueError):
            return value[1:-1]
        return parsed if isinstance(parsed, str) else str(parsed)

    if " #" in value:
        value = value.split(" #", 1)[0].rstrip()
    return value


def _parse_dotenv_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None

    match = _ASSIGNMENT_RE.match(line)
    if match is None:
        return None
    return match.group("key"), _parse_dotenv_value(match.group("value"))


def _candidate_dotenv_paths() -> list[Path]:
    candidates: list[Path] = []
    seen: set[Path] = set()

    for base in [Path.cwd(), Path(__file__).resolve()]:
        current = base if base.is_dir() else base.parent
        for parent in [current, *current.parents]:
            candidate = parent / ".env"
            if candidate in seen:
                continue
            seen.add(candidate)
            candidates.append(candidate)
    return candidates


def find_repo_dotenv() -> Path | None:
    explicit = os.environ.get("PREDEX_ENV_FILE", "").strip()
    if explicit:
        candidate = Path(explicit).expanduser().resolve()
        return candidate if candidate.is_file() else None

    for candidate in _candidate_dotenv_paths():
        if candidate.is_file():
            return candidate
    return None


def load_repo_dotenv(*, override: bool = False) -> Path | None:
    dotenv_path = find_repo_dotenv()
    if dotenv_path is None:
        return None

    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_dotenv_line(line)
        if parsed is None:
            continue
        key, value = parsed
        if override or key not in os.environ:
            os.environ[key] = value
    return dotenv_path


__all__ = ["find_repo_dotenv", "load_repo_dotenv"]
