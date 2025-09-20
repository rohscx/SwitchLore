"""Utilities for ingesting switch configuration files."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Union

PathLike = Union[str, Path]


class SwitchLoreBase:
    """Base class responsible for ingesting switch configuration files.

    Parameters
    ----------
    sources:
        A path or collection of paths to configuration files or directories
        containing configuration files.
    extension:
        Optional file extension filter (e.g. ``".cfg"``). Files that do not
        end with the extension are ignored.
    exclude:
        Optional list of regular expression patterns. Files or directories
        whose names match any of the patterns are ignored.
    """

    def ingest_files(
        self,
        sources: Union[PathLike, Iterable[PathLike]],
        extension: Optional[str] = None,
        exclude: Optional[Sequence[str]] = None,
    ) -> List[Path]:
        """Expand ``sources`` into a list of file paths ready for parsing.

        The method accepts both individual files and directories. Directories
        are searched recursively while applying the provided filters.

        Parameters
        ----------
        sources:
            Path(s) to process. Directories are traversed recursively.
        extension:
            Optional filename suffix used to filter files.
        exclude:
            Optional iterable of regular expression patterns used to skip
            matching file or directory names.

        Returns
        -------
        list[pathlib.Path]
            A list of absolute file paths that matched the provided filters.

        Raises
        ------
        ValueError
            If any of the provided sources do not exist or are not regular
            files/directories.
        """

        compiled_excludes = [re.compile(pattern) for pattern in (exclude or [])]

        def is_excluded(name: str) -> bool:
            return any(pattern.search(name) for pattern in compiled_excludes)

        candidates: Iterable[PathLike]
        if isinstance(sources, (str, Path)):
            candidates = [sources]
        else:
            candidates = sources

        resolved_sources = [Path(source).expanduser() for source in candidates]

        matched_files: List[Path] = []
        for source in resolved_sources:
            if not source.exists():
                raise ValueError(f"'{source}' does not exist")

            if source.is_dir():
                for root, dirs, files in os.walk(source):
                    dirs[:] = [d for d in dirs if not is_excluded(d)]

                    for fname in files:
                        if is_excluded(fname):
                            continue
                        if extension and not fname.endswith(extension):
                            continue
                        matched_files.append(Path(root, fname).resolve())
            elif source.is_file():
                if is_excluded(source.name):
                    continue
                if extension and not source.name.endswith(extension):
                    continue
                matched_files.append(source.resolve())
            else:
                raise ValueError(
                    f"'{source}' is neither a regular file nor a directory"
                )

        return matched_files

    def __init__(
        self,
        sources: Union[PathLike, Iterable[PathLike]],
        extension: Optional[str] = None,
        exclude: Optional[Sequence[str]] = None,
    ) -> None:
        self._extension = extension
        self._exclude = list(exclude or [])
        self._sources = sources
        self.files = self.ingest_files(sources, extension=extension, exclude=exclude)

    @property
    def extension(self) -> Optional[str]:
        """Return the configured extension filter."""

        return self._extension

    @property
    def exclude(self) -> Sequence[str]:
        """Return the configured exclusion patterns."""

        return tuple(self._exclude)

    @property
    def sources(self) -> Union[PathLike, Iterable[PathLike]]:
        """Return the original sources provided at initialization."""

        return self._sources
