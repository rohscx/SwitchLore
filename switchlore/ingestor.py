"""Utilities for ingesting switch configuration files."""

from __future__ import annotations

import os
import re
import shlex
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Pattern,
    Sequence,
    Union,
)

import pandas as pd
from ntc_templates.parse import parse_output

PathLike = Union[str, Path]
SectionSplitter = Callable[[str], Optional[str]]
SectionMapping = Dict[str, str]
SectionsByFile = Dict[Path, SectionMapping]
CommandSpec = Union[str, Mapping[str, Any]]


@dataclass(frozen=True)
class CommandRequest:
    """Normalized command specification consumed by :meth:`SwitchLore.query`."""

    command: str
    action: str = "parse"
    options: Mapping[str, Any] = field(default_factory=dict)


ActionHandler = Callable[
    [Path, str, str, bool, bool, str, Mapping[str, Any]],
    List[Dict[str, Any]],
]


_VALUE_CHARS = set(".:/[]{}()%")
_VALUE_KEYWORDS = {
    "auto",
    "on",
    "off",
    "enable",
    "disable",
    "true",
    "false",
    "full",
    "half",
    "up",
    "down",
    "primary",
    "secondary",
    "active",
    "passive",
    "manual",
    "dynamic",
    "in",
    "out",
}


def _tokenize_config_line(line: str) -> List[str]:
    """Return tokens for ``line`` preserving quoted strings."""

    lexer = shlex.shlex(line, posix=True)
    lexer.commenters = ""
    lexer.whitespace_split = True
    return [token.strip() for token in lexer if token.strip()]


def _looks_like_value(token: str) -> bool:
    """Heuristic to determine if ``token`` represents a configuration value."""

    if any(char.isdigit() for char in token):
        return True
    if any(char in _VALUE_CHARS for char in token):
        return True
    if token.upper() == token and token.lower() != token:
        return True
    if token.lower() in _VALUE_KEYWORDS:
        return True
    return False


def _extract_config_item(line: str) -> Optional[tuple[str, Any]]:
    """Return a configuration key/value pair derived from ``line``.

    The returned key is a normalized command string while the value corresponds
    to the command argument or a boolean flag when no explicit argument is
    present. ``None`` is returned for empty/comment lines.
    """

    stripped = line.strip()
    if not stripped or stripped.startswith("!"):
        return None

    normalized = re.sub(r"\s+", " ", stripped)
    lowered = normalized.lower()

    if lowered.startswith("no "):
        remainder = normalized[3:].strip()
        if not remainder:
            return normalized, False
        tokens = _tokenize_config_line(remainder)
        if not tokens:
            return remainder, False
        if len(tokens) >= 2 and tokens[0].lower() in {"ip", "ipv6"} and tokens[1].lower() == "address":
            key = " ".join(tokens[:2])
        else:
            key = " ".join(tokens)
        return key, False

    if lowered.startswith("default "):
        remainder = normalized[len("default ") :].strip()
        if not remainder:
            return normalized, "default"
        tokens = _tokenize_config_line(remainder)
        if not tokens:
            return remainder, "default"
        if len(tokens) >= 2 and tokens[0].lower() in {"ip", "ipv6"} and tokens[1].lower() == "address":
            key = " ".join(tokens[:2])
        else:
            key = " ".join(tokens)
        return key, "default"

    if ":" in normalized:
        first_space = normalized.find(" ")
        colon_index = normalized.find(":")
        if first_space == -1 or colon_index < first_space:
            key, _, value = normalized.partition(":")
            key = key.strip()
            value = value.strip()
            if key:
                return key, value or True

    tokens = _tokenize_config_line(normalized)
    if not tokens:
        return None

    if len(tokens) == 1:
        return tokens[0], True

    first_token_lower = tokens[0].lower()
    if first_token_lower in {"description", "alias", "name"}:
        value = " ".join(tokens[1:])
        return tokens[0], value or True

    if tokens[0].lower() in {"ip", "ipv6"} and len(tokens) >= 3 and tokens[1].lower() == "address":
        key = " ".join(tokens[:2])
        value = " ".join(tokens[2:])
        return key, value or True

    if len(tokens) == 2:
        second = tokens[1]
        if (
            any(char.isdigit() for char in second)
            or any(char.isupper() for char in second)
            or second.lower() in _VALUE_KEYWORDS
            or tokens[0].lower() in {"description", "alias", "name", "duplex", "speed"}
        ):
            return tokens[0], second
        return " ".join(tokens), True

    for index, token in enumerate(tokens[1:], start=1):
        if any(char.isupper() for char in token) and token.lower() != token:
            key = " ".join(tokens[:index])
            value = " ".join(tokens[index:])
            if key:
                return key, value or True

    key_tokens = tokens[:]
    value_tokens: List[str] = []
    while key_tokens:
        candidate = key_tokens[-1]
        if not value_tokens:
            value_tokens.insert(0, candidate)
            key_tokens.pop()
            if not _looks_like_value(candidate):
                break
            continue
        if _looks_like_value(candidate):
            value_tokens.insert(0, candidate)
            key_tokens.pop()
        else:
            break

    if not key_tokens:
        key = tokens[0]
        value = " ".join(tokens[1:])
        return key, value or True

    if len(key_tokens) > 1:
        normalized_key_tokens: List[str] = []
        moved_tokens: List[str] = []
        for index, token in enumerate(key_tokens):
            if index == 0:
                normalized_key_tokens.append(token)
                continue
            if _looks_like_value(token):
                moved_tokens.append(token)
            else:
                normalized_key_tokens.append(token)
        if moved_tokens:
            key_tokens = normalized_key_tokens or key_tokens
            value_tokens = moved_tokens + value_tokens

    key = " ".join(key_tokens)
    value = " ".join(value_tokens)
    return key, value or True


_ACTION_ALIASES = {
    "parse": "parse",
    "ntc": "parse",
    "ntc_parse": "parse",
    "capture_interface_config": "capture_interface_config",
    "capture_interface_configuration": "capture_interface_config",
    "capture_interfaces": "capture_interface_config",
    "interface_config": "capture_interface_config",
    "interface_configuration": "capture_interface_config",
}

_SUPPORTED_ACTIONS = set(_ACTION_ALIASES.values())


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
        commands: Union[CommandSpec, Sequence[CommandSpec]],
        *,
        platform: str = "cisco_ios",
        include_raw: bool = False,
        strict: bool = False,
    ) -> pd.DataFrame:
        """Return a :class:`pandas.DataFrame` with processed command outputs.

        Parameters
        ----------
        commands:
            Command specification or sequence of specifications. Each entry
            can be either a command name (section title) or a mapping
            describing the section and the action to perform. For example::

                [
                    "show cdp neighbors detail",
                    {
                        "section": "show running-config interface",
                        "action": "capture_interface_config",
                        "options": {"terminators": ["exit"]},
                    },
                ]

            Passing a single mapping without wrapping it in a list is also
            supported. When omitted, ``action`` defaults to ``"parse"`` which
            leverages :mod:`ntc_templates`.
            Action mappings may include an ``"options"`` dictionary to pass
            handler-specific keyword arguments.
        platform:
            Network operating system passed to
            :func:`ntc_templates.parse.parse_output`.
        include_raw:
            When ``True``, include the raw command output for each row.
        strict:
            When ``True``, exceptions from the ``parse`` action are raised.
            Otherwise they are captured and returned as part of the dataframe.
        """

        if isinstance(commands, MappingABC):
            command_specs: Iterable[CommandSpec] = [commands]
        else:
            command_specs = commands

        if isinstance(command_specs, str):
            raise TypeError(
                "'commands' must be an iterable of command specifications"
            )

        requests = self._normalize_command_requests(command_specs)

        if not requests:
            raise ValueError("At least one command must be provided")

        self._ensure_sections_loaded()

        records: List[Dict[str, Any]] = []

        for source_path, sections in self.sections.items():
            for request in requests:
                content = sections.get(request.command)
                if content is None:
                    continue
                handler = self._resolve_action_handler(request.action)
                new_records = handler(
                    source_path,
                    request.command,
                    content,
                    include_raw,
                    strict,
                    platform,
                    request.options,
                )
                records.extend(new_records)

        if records:
            return pd.DataFrame.from_records(records)

        base_columns = ["source", "command"]
        if include_raw:
            base_columns.append("raw")
        return pd.DataFrame(columns=base_columns)

    def _normalize_command_requests(
        self, commands: Iterable[CommandSpec]
    ) -> List[CommandRequest]:
        """Return validated command requests preserving the original order."""

        requests: List[CommandRequest] = []
        parse_seen: set[str] = set()

        for spec in commands:
            if isinstance(spec, str):
                normalized = spec.strip()
                if not normalized or normalized in parse_seen:
                    continue
                parse_seen.add(normalized)
                requests.append(CommandRequest(command=normalized))
                continue

            if not isinstance(spec, MappingABC):
                raise TypeError(
                    "Command entries must be strings or mappings with a 'section'/'command' key"
                )

            if "section" in spec:
                command_value = spec["section"]
            elif "command" in spec:
                command_value = spec["command"]
            else:
                raise ValueError(
                    "Command mappings must include a 'section' or 'command' entry"
                )

            if not isinstance(command_value, str):
                raise TypeError("Command names must be strings")

            command_name = command_value.strip()
            if not command_name:
                raise ValueError("Command names must be non-empty strings")

            action_value = spec.get("action", "parse")
            if not isinstance(action_value, str):
                raise TypeError("'action' must be a string when provided")

            normalized_action = _ACTION_ALIASES.get(
                action_value.strip().lower(), action_value.strip().lower()
            )

            if normalized_action not in _SUPPORTED_ACTIONS:
                raise ValueError(f"Unsupported action '{action_value}'")

            options_value = spec.get("options", {})
            if options_value is None:
                options_dict: Dict[str, Any] = {}
            elif isinstance(options_value, MappingABC):
                options_dict = dict(options_value)
            else:
                raise TypeError("'options' must be a mapping when provided")

            if normalized_action == "parse" and not options_dict:
                if command_name in parse_seen:
                    continue
                parse_seen.add(command_name)

            requests.append(
                CommandRequest(
                    command=command_name,
                    action=normalized_action,
                    options=options_dict,
                )
            )

        return requests

    def _resolve_action_handler(self, action: str) -> ActionHandler:
        """Return the callable responsible for ``action``."""

        if action == "parse":
            return self._handle_parse_action
        if action == "capture_interface_config":
            return self._handle_capture_interface_config
        raise ValueError(f"Unsupported action '{action}'")

    def _handle_parse_action(
        self,
        source_path: Path,
        command: str,
        content: str,
        include_raw: bool,
        strict: bool,
        platform: str,
        options: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        """Return records generated by :mod:`ntc_templates` parsing."""

        records: List[Dict[str, Any]] = []
        effective_platform = str(options.get("platform", platform))

        try:
            parsed_result = parse_output(
                platform=effective_platform, command=command, data=content
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
            return records

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
            return records

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

        return records

    def _handle_capture_interface_config(
        self,
        source_path: Path,
        command: str,
        content: str,
        include_raw: bool,
        strict: bool,
        platform: str,
        options: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        """Capture interface configuration blocks from ``content``."""

        del strict, platform  # parameters reserved for future use

        pattern_value = options.get("interface_pattern")
        if pattern_value is None:
            interface_pattern: Pattern[str] = re.compile(
                r"^interface\s+(.+)$", re.IGNORECASE
            )
        else:
            if isinstance(pattern_value, str):
                interface_pattern = re.compile(pattern_value, re.IGNORECASE)
            elif isinstance(pattern_value, re.Pattern):
                interface_pattern = pattern_value
            else:
                raise TypeError(
                    "'interface_pattern' must be a string or compiled regular expression"
                )

        if getattr(interface_pattern, "groups", 0) < 1:
            raise ValueError(
                "'interface_pattern' must include at least one capturing group"
            )

        terminators_value = options.get("terminators")
        if terminators_value is None:
            terminators: set[str] = {"exit"}
        else:
            if not isinstance(terminators_value, Sequence) or isinstance(
                terminators_value, (str, bytes)
            ):
                raise TypeError("'terminators' must be a sequence of strings")
            terminators = {
                str(term).strip().lower()
                for term in terminators_value
                if str(term).strip()
            }

        records: List[Dict[str, Any]] = []
        current_interface: Optional[str] = None
        current_header: Optional[str] = None
        current_lines: List[str] = []

        def flush() -> None:
            nonlocal current_interface, current_header, current_lines
            if current_interface is None:
                return
            header = (current_header or f"interface {current_interface}").rstrip()
            block_lines = [header]
            block_lines.extend(current_lines)
            configuration = "\n".join(block_lines).strip("\n")
            record: Dict[str, Any] = {
                "source": str(source_path),
                "command": command,
                "interface": current_interface,
                "configuration": configuration,
            }
            item_columns: Dict[str, Any] = {}
            seen_keys: Dict[str, int] = {}
            for item_line in current_lines:
                parsed_item = _extract_config_item(item_line)
                if not parsed_item:
                    continue
                key, value = parsed_item
                normalized_key = re.sub(r"\s+", " ", key.strip())
                if not normalized_key:
                    continue
                occurrence = seen_keys.get(normalized_key, 0)
                seen_keys[normalized_key] = occurrence + 1
                if occurrence:
                    column_name = f"{normalized_key}__{occurrence + 1}"
                else:
                    column_name = normalized_key
                item_columns[column_name] = value
            for column_name, value in item_columns.items():
                candidate = column_name
                suffix = 2
                while candidate in record:
                    candidate = f"{column_name}__{suffix}"
                    suffix += 1
                record[candidate] = value
            if include_raw:
                record["raw"] = configuration
            records.append(record)
            current_interface = None
            current_header = None
            current_lines = []

        for raw_line in content.splitlines():
            stripped = raw_line.strip()
            match = interface_pattern.match(stripped)
            if match:
                flush()
                group_index = match.lastindex or 1
                captured = match.group(group_index)
                current_interface = captured.strip()
                current_header = raw_line.rstrip()
                current_lines = []
                continue

            if current_interface is None:
                continue

            lowered = stripped.lower()
            if stripped.startswith("!") or lowered in terminators:
                flush()
                continue

            if raw_line and not raw_line[0].isspace():
                flush()
                continue

            current_lines.append(raw_line.rstrip())

        flush()

        if not records:
            record = {"source": str(source_path), "command": command}
            if include_raw:
                record["raw"] = content
            records.append(record)

        return records
