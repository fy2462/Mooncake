#!/usr/bin/env python3
"""Discover C++ GoogleTest and Rust test declarations without building them."""

from __future__ import annotations

import argparse
import ast
from collections.abc import Collection
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
import sys
from typing import Iterable


CPP_SUFFIXES = {".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp"}
RUST_SUFFIXES = {".rs"}
PYTHON_SUFFIXES = {".py"}
CPP_TEST_RE = re.compile(
    r"\bTEST(?:_F|_P)?\s*\(\s*([A-Za-z_]\w*)\s*,\s*([A-Za-z_]\w*)\s*\)",
    re.MULTILINE,
)
RUST_TEST_RE = re.compile(
    r"#\s*\[\s*(?:tokio::)?test(?:\s*\([^\]]*\))?\s*\]"
    r"(?:\s*#\s*\[[^\]]+\])*"
    r"\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_]\w*)\s*\(",
    re.MULTILINE,
)


@dataclass(frozen=True, order=True)
class TestRef:
    framework: str
    file: str
    name: str


def _strip_c_like_comments(text: str, *, single_quoted_strings: bool = True) -> str:
    """Remove C/C++ comments while preserving strings and line structure."""
    output: list[str] = []
    index = 0
    state = "code"
    quote = ""
    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""
        if state == "code":
            if char == '"' or (single_quoted_strings and char == "'"):
                state = "string"
                quote = char
                output.append(char)
            elif char == "/" and next_char == "/":
                state = "line_comment"
                output.extend("  ")
                index += 1
            elif char == "/" and next_char == "*":
                state = "block_comment"
                output.extend("  ")
                index += 1
            else:
                output.append(char)
        elif state == "string":
            if char == "\\" and next_char:
                output.extend("  ")
                index += 1
            elif char == quote:
                output.append(char)
                state = "code"
            elif char == "\n":
                output.append(char)
            else:
                output.append(" ")
        elif state == "line_comment":
            if char == "\n":
                output.append(char)
                state = "code"
            else:
                output.append(" ")
        else:
            if char == "*" and next_char == "/":
                output.extend("  ")
                index += 1
                state = "code"
            elif char == "\n":
                output.append(char)
            else:
                output.append(" ")
        index += 1
    return "".join(output)


def _iter_files(root: Path, suffixes: set[str]) -> Iterable[Path]:
    if not root.is_dir():
        raise NotADirectoryError(root)
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in suffixes
    )


def discover_cpp_tests(root: Path) -> list[TestRef]:
    refs: list[TestRef] = []
    for path in _iter_files(root, CPP_SUFFIXES):
        text = _strip_c_like_comments(path.read_text(encoding="utf-8"))
        relative = path.relative_to(root).as_posix()
        refs.extend(
            TestRef("gtest", relative, f"{suite}.{test}")
            for suite, test in CPP_TEST_RE.findall(text)
        )
    return sorted(set(refs))


def discover_rust_tests(root: Path) -> list[TestRef]:
    refs: list[TestRef] = []
    for path in _iter_files(root, RUST_SUFFIXES):
        text = _strip_c_like_comments(
            path.read_text(encoding="utf-8"), single_quoted_strings=False
        )
        relative = path.relative_to(root).as_posix()
        refs.extend(
            TestRef("rust", relative, name) for name in RUST_TEST_RE.findall(text)
        )
    return sorted(set(refs))


def _python_base_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_python_base_name(node.value)}.{node.attr}"
    return ""


def discover_python_tests(
    root: Path,
    excluded_files: Collection[str] = (),
) -> list[TestRef]:
    """Discover pytest/unittest declarations without importing test modules."""
    excluded = set(excluded_files)
    refs: list[TestRef] = []
    for path in _iter_files(root, PYTHON_SUFFIXES):
        relative = path.relative_to(root).as_posix()
        if relative in excluded:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name.startswith("test_"):
                    refs.append(TestRef("python", relative, node.name))
                continue
            if not isinstance(node, ast.ClassDef):
                continue
            is_test_class = node.name.startswith("Test") or any(
                _python_base_name(base).endswith("TestCase") for base in node.bases
            )
            if not is_test_class:
                continue
            refs.extend(
                TestRef("python", relative, f"{node.name}.{child.name}")
                for child in node.body
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                and child.name.startswith("test_")
            )
    return sorted(set(refs))


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp-root", type=Path, required=True)
    parser.add_argument("--rust-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    try:
        payload = {
            "schema_version": 1,
            "cpp": [asdict(ref) for ref in discover_cpp_tests(args.cpp_root)],
            "rust": [asdict(ref) for ref in discover_rust_tests(args.rust_root)],
        }
    except (OSError, UnicodeError) as error:
        print(f"inventory error: {error}", file=sys.stderr)
        return 2

    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
