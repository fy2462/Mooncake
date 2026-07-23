#!/usr/bin/env python3
"""Validate the Rust Store learning site's source contracts."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from xml.etree import ElementTree


DOCS_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = DOCS_ROOT / "source"
REPO_ROOT = DOCS_ROOT.parents[1]
REQUIRED_INDEXES = (
    "getting-started/index.md",
    "architecture/index.md",
    "concepts/index.md",
    "store/index.md",
    "transfer-engine/index.md",
    "walkthroughs/index.md",
    "code-map/index.md",
    "testing/index.md",
    "labs/index.md",
)
PLACEHOLDER = re.compile(r"\b(?:TBD|TODO|FIXME)\b")
CODE_PATH = re.compile(r"`((?:rust-repo/|mooncake-transfer-engine/)[^`:#\s]+)")


def fail(errors: list[str], message: str) -> None:
    errors.append(message)


def validate_xml(errors: list[str], path: Path) -> None:
    try:
        ElementTree.parse(path)
    except (OSError, ElementTree.ParseError) as error:
        fail(errors, f"invalid XML {path.relative_to(DOCS_ROOT)}: {error}")


def main() -> int:
    errors: list[str] = []
    root_index = SOURCE_ROOT / "index.md"
    if not root_index.is_file():
        fail(errors, "missing source/index.md")
    for relative in REQUIRED_INDEXES:
        if not (SOURCE_ROOT / relative).is_file():
            fail(errors, f"missing source/{relative}")

    markdown_files = sorted(SOURCE_ROOT.rglob("*.md")) if SOURCE_ROOT.exists() else []
    for path in markdown_files:
        text = path.read_text(encoding="utf-8")
        match = PLACEHOLDER.search(text)
        if match:
            fail(
                errors,
                f"placeholder {match.group(0)} in {path.relative_to(DOCS_ROOT)}",
            )
        for raw_path in CODE_PATH.findall(text):
            target = REPO_ROOT / raw_path
            if not target.exists():
                fail(
                    errors,
                    f"missing source path {raw_path} referenced by "
                    f"{path.relative_to(DOCS_ROOT)}",
                )

    diagrams = DOCS_ROOT / "diagrams"
    static_diagrams = SOURCE_ROOT / "_static" / "diagrams"
    for drawio in sorted(diagrams.glob("*.drawio")) if diagrams.exists() else []:
        svg = static_diagrams / f"{drawio.stem}.svg"
        if not svg.is_file():
            fail(errors, f"missing SVG pair for diagrams/{drawio.name}")
        validate_xml(errors, drawio)
        if svg.is_file():
            validate_xml(errors, svg)

    if errors:
        print("documentation contract failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print(
        f"documentation contract passed: {len(markdown_files)} Markdown files",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
