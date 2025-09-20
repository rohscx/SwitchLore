"""Utilities for ingesting switch configuration files."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping as MappingABC
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Union

import pandas as pd
from ntc_templates.parse import parse_output

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

    def iter_sections(
        self, commands: Optional[Sequence[str]] = None
    ) -> Iterator[tuple[Path, str, str]]:
        """Yield ``(file_path, section_name, content)`` tuples.

        Parameters
        ----------
        commands:
            Optional sequence of section names (commands) to yield. When
            provided, only matching sections are returned in the order given
            for every file. If omitted, all available sections for every file
            are yielded.
        """

        if commands is None:
            for file_path, sections in self.sections.items():
                for section_name, content in sections.items():
                    yield file_path, section_name, content
            return

        for file_path, sections in self.sections.items():
            for command in commands:
                content = sections.get(command)
                if content is not None:
                    yield file_path, command, content


class SwitchLore(SwitchLoreBase):
    """Extended ingestor that can parse sections into tabular data."""

    def __init__(
        self,
        sources: Union[PathLike, Iterable[PathLike]],
        extension: Optional[str] = None,
        exclude: Optional[Sequence[str]] = None,
        *,
        auto_load: bool = True,
    ) -> None:
        super().__init__(sources, extension=extension, exclude=exclude)
        if auto_load:
            self.load_sections()

    def _ensure_sections_loaded(self) -> None:
        if not self.sections:
            self.load_sections()

    def query(
        self,
        commands: Sequence[str],
        *,
        platform: str = "cisco_ios",
        include_raw: bool = False,
        strict: bool = False,
    ) -> pd.DataFrame:
        """Return a :class:`pandas.DataFrame` with parsed command outputs.

        Parameters
        ----------
        commands:
            Sequence of command names (section titles) to parse.
        platform:
            Network operating system passed to
            :func:`ntc_templates.parse.parse_output`.
        include_raw:
            When ``True``, include the raw command output for each row.
        strict:
            When ``True``, exceptions from :func:`parse_output` are raised.
            Otherwise they are captured and returned as part of the dataframe.
        """

        if isinstance(commands, str):
            raise TypeError("'commands' must be an iterable of command strings")

        unique_commands: List[str] = []
        seen = set()
        for command in commands:
            if not isinstance(command, str):
                raise TypeError("All command names must be strings")
            normalized = command.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            unique_commands.append(normalized)

        if not unique_commands:
            raise ValueError("At least one command must be provided")

        self._ensure_sections_loaded()

        records: List[Dict[str, Any]] = []

        for source_path, command, content in self.iter_sections(unique_commands):
            try:
                parsed_result = parse_output(
                    platform=platform, command=command, data=content
                )
            except Exception as exc:  # noqa: BLE001 - surface parsing issues
                if strict:
                    raise
                record: Dict[str, Any] = {
                    "source": str(source_path),
                    "command": command,
                    "error": str(exc),
                }
                if include_raw:
                    record["raw"] = content
                records.append(record)
                continue

            if parsed_result is None:
                parsed_rows: List[Any] = []
            elif isinstance(parsed_result, list):
                parsed_rows = parsed_result
            else:
                parsed_rows = [parsed_result]

            if not parsed_rows:
                record = {"source": str(source_path), "command": command}
                if include_raw:
                    record["raw"] = content
                records.append(record)
                continue

            for row_data in parsed_rows:
                row: Dict[str, Any] = {
                    "source": str(source_path),
                    "command": command,
                }
                if isinstance(row_data, MappingABC):
                    row.update(row_data)
                else:
                    row["value"] = row_data
                if include_raw:
                    row["raw"] = content
                records.append(row)

        if records:
            return pd.DataFrame.from_records(records)

        base_columns = ["source", "command"]
        if include_raw:
            base_columns.append("raw")
        return pd.DataFrame(columns=base_columns)
