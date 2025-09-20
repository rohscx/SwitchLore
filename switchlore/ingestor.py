"""Utilities for ingesting switch configuration files."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Union

PathLike = Union[str, Path]
SectionSplitter = Callable[[str], Optional[str]]
SectionMapping = Dict[str, str]
SectionsByFile = Dict[Path, SectionMapping]


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
        self._sections: SectionsByFile = {}

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

    @property
    def sections(self) -> Mapping[Path, Mapping[str, str]]:
        """Return the parsed configuration sections."""

        return self._sections

    @staticmethod
    def _default_section_splitter(line: str) -> Optional[str]:
        """Return the section name if ``line`` denotes a new section."""

        if line.startswith("---") and "show " in line:
            return line.strip("- ").strip()
        return None

    def load_sections(
        self,
        section_splitter: Optional[SectionSplitter] = None,
        *,
        encoding: str = "utf-8",
        ) -> None:
      
        """Parse configuration files into named sections.

        Parameters
        ----------
        section_splitter:
            Optional callable invoked for each line in the file. The callable
            receives the line (with trailing whitespace removed) and should
            return the section name when a new section starts, or ``None``
            otherwise. If omitted, lines starting with ``"---"`` and containing
            ``"show "`` mark new sections, matching the behaviour of the
            original :func:`parse_conf_file` helper.
        encoding:
            Text encoding used when reading configuration files."""

        splitter = section_splitter or self._default_section_splitter
        parsed_sections: SectionsByFile = {}

        for file_path in self.files:
            sections: SectionMapping = {}
            current_section: Optional[str] = None
            current_content: List[str] = []

            with file_path.open("r", encoding=encoding) as file_obj:
                for raw_line in file_obj:
                    line = raw_line.rstrip()
                    section_name = splitter(line)

                    if section_name is not None:
                        if current_section is not None:
                            sections[current_section] = "\n".join(current_content)
                        current_section = section_name
                        current_content = []
                    elif current_section is not None:
                        current_content.append(line)

                if current_section is not None and current_content:
                    sections[current_section] = "\n".join(current_content)

            parsed_sections[file_path] = sections

        self._sections = parsed_sections
