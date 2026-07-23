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
REQUIRED_PAGES = (
    "architecture/system-overview.md",
    "architecture/control-plane-and-data-plane.md",
    "concepts/rdma-for-rust-developers.md",
    "concepts/object-replica-and-segment.md",
    "concepts/multilevel-cache.md",
    "store/client.md",
    "store/master.md",
    "store/ha-and-recovery.md",
    "transfer-engine/ffi-boundary.md",
    "transfer-engine/transport-and-rdma.md",
    "walkthroughs/put-object.md",
    "walkthroughs/get-object.md",
    "walkthroughs/remove-object.md",
    "testing/single-node.md",
    "testing/multi-node.md",
    "labs/running-three-node-e2e.md",
)
LESSON_HEADINGS = ("前置章节", "本章目标", "自检问题", "下一步")
REQUIRED_TOPICS = (
    "PutStart",
    "PutEnd",
    "PutRevoke",
    "LocalDisk",
    "promotion",
    "lag_entries",
    "TransferEngine",
    "protocol=rdma",
)
PLACEHOLDER = re.compile(r"\b(?:TBD|TODO|FIXME)\b")
CODE_PATH = re.compile(r"`((?:rust-repo/|mooncake-transfer-engine/)[^`:#\s]+)")
TOCTREE = re.compile(r"```\{toctree\}\n(?P<body>.*?)```", re.DOTALL)


def fail(errors: list[str], message: str) -> None:
    errors.append(message)


def validate_xml(errors: list[str], path: Path) -> None:
    try:
        ElementTree.parse(path)
    except (OSError, ElementTree.ParseError) as error:
        fail(errors, f"invalid XML {path.relative_to(DOCS_ROOT)}: {error}")


def validate_toctrees(errors: list[str], path: Path, text: str) -> None:
    for block in TOCTREE.finditer(text):
        for line in block.group("body").splitlines():
            entry = line.strip()
            if not entry or entry.startswith(":"):
                continue
            if "<" in entry and entry.endswith(">"):
                entry = entry.rsplit("<", 1)[1][:-1].strip()
            if entry.startswith(("http://", "https://")):
                continue
            target = SOURCE_ROOT / entry.lstrip("/") if entry.startswith("/") else path.parent / entry
            candidates = (target.with_suffix(".md"), target / "index.md")
            if not any(candidate.is_file() for candidate in candidates):
                fail(
                    errors,
                    f"missing toctree target {entry!r} in {path.relative_to(DOCS_ROOT)}",
                )


def main() -> int:
    errors: list[str] = []
    root_index = SOURCE_ROOT / "index.md"
    if not root_index.is_file():
        fail(errors, "missing source/index.md")
    for relative in REQUIRED_INDEXES:
        if not (SOURCE_ROOT / relative).is_file():
            fail(errors, f"missing source/{relative}")
    for relative in REQUIRED_PAGES:
        if not (SOURCE_ROOT / relative).is_file():
            fail(errors, f"missing required lesson source/{relative}")

    markdown_files = sorted(SOURCE_ROOT.rglob("*.md")) if SOURCE_ROOT.exists() else []
    corpus = ""
    mermaid_blocks = 0
    for path in markdown_files:
        text = path.read_text(encoding="utf-8")
        corpus += text
        mermaid_blocks += text.count("```{mermaid}")
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
        validate_toctrees(errors, path, text)
        if path.name != "index.md":
            for heading in LESSON_HEADINGS:
                if f"## {heading}" not in text:
                    fail(
                        errors,
                        f"missing lesson heading '## {heading}' in "
                        f"{path.relative_to(DOCS_ROOT)}",
                    )

    for topic in REQUIRED_TOPICS:
        if topic not in corpus:
            fail(errors, f"required topic not covered: {topic}")
    if mermaid_blocks < 8:
        fail(errors, f"expected at least 8 Mermaid blocks, found {mermaid_blocks}")

    diagrams = DOCS_ROOT / "diagrams"
    static_diagrams = SOURCE_ROOT / "_static" / "diagrams"
    drawio_files = sorted(diagrams.glob("*.drawio")) if diagrams.exists() else []
    if len(drawio_files) < 2:
        fail(errors, f"expected at least 2 Draw.io sources, found {len(drawio_files)}")
    for drawio in drawio_files:
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
