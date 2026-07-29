"""自动验证 Markdown 文档中的 Python 示例。"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest
from markdown_it import MarkdownIt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOC_PATHS = ("README.md", "docs/API参考.md")
DOC_EXAMPLE_TIMEOUT_SECONDS = 5.0
PythonExecution = Literal["runnable", "compile-only"]

_DOC_EXAMPLE_RUNNER = """\
import sys

filename = sys.argv[1]
line_offset = int(sys.argv[2])
source = "\\n" * line_offset + sys.stdin.read()
globals()["__file__"] = filename
exec(compile(source, filename, "exec", dont_inherit=True), globals())
"""


@dataclass(frozen=True)
class MarkdownBlock:
    path: str
    line: int
    language: str
    options: tuple[str, ...]
    code: str


def _load_markdown_blocks(path: str) -> list[MarkdownBlock]:
    content = (PROJECT_ROOT / path).read_text(encoding="utf-8")
    blocks: list[MarkdownBlock] = []

    for token in MarkdownIt("commonmark").parse(content):
        if token.type != "fence":
            continue
        info = token.info.split()
        language = info[0] if info else ""
        blocks.append(
            MarkdownBlock(
                path=path,
                line=token.map[0] + 1 if token.map else 1,
                language=language,
                options=tuple(info[1:]),
                code=token.content,
            )
        )

    return blocks


def _load_blocks_by_language(language: str) -> list[MarkdownBlock]:
    return [
        block
        for path in DOC_PATHS
        for block in _load_markdown_blocks(path)
        if block.language == language
    ]


def _block_id(block: MarkdownBlock) -> str:
    return f"{block.path}:{block.line}"


def _python_execution(block: MarkdownBlock) -> PythonExecution:
    if not block.options:
        return "runnable"
    if block.options == ("compile-only",):
        return "compile-only"
    raise AssertionError(
        f"Unsupported Python fence options at {_block_id(block)}: {block.options!r}"
    )


def _compile_example(block: MarkdownBlock) -> None:
    compile(
        block.code,
        f"{block.path}:{block.line}",
        "exec",
        dont_inherit=True,
    )


def _output_text(output: str | bytes | None) -> str:
    if isinstance(output, bytes):
        return output.decode(errors="replace")
    return output or ""


def _example_failure(
    block: MarkdownBlock,
    summary: str,
    *,
    stdout: str | bytes | None,
    stderr: str | bytes | None,
) -> AssertionError:
    return AssertionError(
        f"Markdown example at {_block_id(block)} {summary}\n"
        f"stdout:\n{_output_text(stdout)}\n"
        f"stderr:\n{_output_text(stderr)}"
    )


def _execute_example(block: MarkdownBlock) -> None:
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-X",
                "utf8",
                "-c",
                _DOC_EXAMPLE_RUNNER,
                block.path,
                str(block.line),
            ],
            input=block.code,
            encoding="utf-8",
            capture_output=True,
            cwd=PROJECT_ROOT,
            timeout=DOC_EXAMPLE_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise _example_failure(
            block,
            f"timed out after {DOC_EXAMPLE_TIMEOUT_SECONDS:g} seconds",
            stdout=exc.stdout,
            stderr=exc.stderr,
        ) from exc

    if result.returncode != 0:
        raise _example_failure(
            block,
            f"failed with exit code {result.returncode}",
            stdout=result.stdout,
            stderr=result.stderr,
        )


PYTHON_BLOCKS = _load_blocks_by_language("python")


@pytest.mark.parametrize("block", PYTHON_BLOCKS, ids=_block_id)
def test_documented_python_block(block: MarkdownBlock) -> None:
    if _python_execution(block) == "compile-only":
        _compile_example(block)
    else:
        _execute_example(block)
