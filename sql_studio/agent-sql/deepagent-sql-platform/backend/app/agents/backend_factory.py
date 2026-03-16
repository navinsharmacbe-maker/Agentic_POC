from pathlib import Path
from typing import Any

from deepagents.backends import CompositeBackend, FilesystemBackend, StateBackend
from deepagents.backends.protocol import BackendProtocol, EditResult, WriteResult
from deepagents.backends.utils import FileInfo, GrepMatch

from app.config import AGENT_MEMORIES_DIR, AGENT_WORKSPACE_DIR, AGENT_DENY_WRITE_PREFIXES


class PolicyWrapper(BackendProtocol):
    def __init__(self, inner: BackendProtocol, deny_prefixes: list[str] | None = None):
        self.inner = inner
        self.deny_prefixes = [p if p.endswith("/") else p + "/" for p in (deny_prefixes or [])]

    def _deny(self, path: str) -> bool:
        return any(path.startswith(prefix) for prefix in self.deny_prefixes)

    def ls_info(self, path: str) -> list[FileInfo]:
        return self.inner.ls_info(path)

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> str:
        return self.inner.read(file_path, offset=offset, limit=limit)

    def grep_raw(self, pattern: str, path: str | None = None, glob: str | None = None) -> list[GrepMatch] | str:
        return self.inner.grep_raw(pattern, path, glob)

    def glob_info(self, pattern: str, path: str = "/") -> list[FileInfo]:
        return self.inner.glob_info(pattern, path)

    def write(self, file_path: str, content: str) -> WriteResult:
        if self._deny(file_path):
            return WriteResult(error=f"Writes are not allowed under {file_path}")
        return self.inner.write(file_path, content)

    def edit(self, file_path: str, old_string: str, new_string: str, replace_all: bool = False) -> EditResult:
        if self._deny(file_path):
            return EditResult(error=f"Edits are not allowed under {file_path}")
        return self.inner.edit(file_path, old_string, new_string, replace_all)


def build_backend(runtime: Any) -> BackendProtocol:
    workspace_root = Path(AGENT_WORKSPACE_DIR)
    memories_root = Path(AGENT_MEMORIES_DIR)
    workspace_root.mkdir(parents=True, exist_ok=True)
    memories_root.mkdir(parents=True, exist_ok=True)

    backend = CompositeBackend(
        default=StateBackend(runtime),
        routes={
            "/workspace/": FilesystemBackend(root_dir=str(workspace_root), virtual_mode=True),
            "/memories/": FilesystemBackend(root_dir=str(memories_root), virtual_mode=True),
        },
    )

    return PolicyWrapper(backend, deny_prefixes=AGENT_DENY_WRITE_PREFIXES)
