"""Tests for the :mod:`switchlore.ingestor` module."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from switchlore import SwitchLore


class TestSwitchLoreQuery(TestCase):
    """Unit tests covering the :meth:`SwitchLore.query` method."""

    def setUp(self) -> None:
        super().setUp()
        self._tmpdir = TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.base_path = Path(self._tmpdir.name)

    def _write_config(self, lines: Iterable[str]) -> Path:
        path = self.base_path / "switch.cfg"
        content = "\n".join(lines)
        path.write_text(content, encoding="utf-8")
        return path

    def test_query_with_parse_action_uses_ntc_templates(self) -> None:
        """String commands are still parsed using ``ntc_templates``."""

        config_path = self._write_config(
            [
                "--- show cdp neighbors detail",
                "Device ID: SwitchA",
            ]
        )

        ingestor = SwitchLore(config_path)

        with patch(
            "switchlore.ingestor.parse_output",
            return_value=[{"neighbor": "SwitchA"}],
        ) as mock_parse:
            df = ingestor.query(["show cdp neighbors detail"])

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        row = df.iloc[0]
        self.assertEqual(row["neighbor"], "SwitchA")
        self.assertEqual(row["command"], "show cdp neighbors detail")
        self.assertEqual(row["source"], str(config_path.resolve()))

        mock_parse.assert_called_once_with(
            platform="cisco_ios",
            command="show cdp neighbors detail",
            data="Device ID: SwitchA",
        )

    def test_capture_interface_config_action_returns_interface_blocks(self) -> None:
        """Custom action extracts interface configuration blocks."""

        config_path = self._write_config(
            [
                "--- show running-config interface",
                "interface GigabitEthernet1/0/1",
                " description Uplink",
                " switchport access vlan 10",
                "!",
                "interface Vlan10",
                " ip address 10.10.10.1 255.255.255.0",
                " no shutdown",
                "exit",
                "interface GigabitEthernet1/0/2",
                " shutdown",
            ]
        )

        ingestor = SwitchLore(config_path)

        df = ingestor.query(
            [
                {
                    "section": "show running-config interface",
                    "action": "capture_interface_config",
                }
            ]
        )

        self.assertEqual(df["interface"].tolist(), [
            "GigabitEthernet1/0/1",
            "Vlan10",
            "GigabitEthernet1/0/2",
        ])

        config_values = df["configuration"].tolist()
        self.assertTrue(config_values[0].startswith("interface GigabitEthernet1/0/1"))
        self.assertIn("description Uplink", config_values[0])
        self.assertIn("ip address 10.10.10.1 255.255.255.0", config_values[1])
        self.assertIn("shutdown", config_values[2])
        self.assertNotIn("raw", df.columns)

        gig1 = df[df["interface"] == "GigabitEthernet1/0/1"].iloc[0]
        self.assertEqual(gig1["description"], "Uplink")
        self.assertEqual(gig1["switchport access vlan"], "10")

        vlan10 = df[df["interface"] == "Vlan10"].iloc[0]
        self.assertEqual(
            vlan10["ip address"],
            "10.10.10.1 255.255.255.0",
        )
        self.assertFalse(vlan10["shutdown"])

        gig2 = df[df["interface"] == "GigabitEthernet1/0/2"].iloc[0]
        self.assertTrue(gig2["shutdown"])

    def test_capture_interface_config_preserves_colon_values(self) -> None:
        """Interface values containing colons are not truncated."""

        config_path = self._write_config(
            [
                "--- show running-config interface",
                "interface GigabitEthernet1/0/3",
                " description Link to ISP: Primary",
                " ipv6 address 2001:db8::1/64",
                "!",
            ]
        )

        ingestor = SwitchLore(config_path)

        df = ingestor.query(
            {
                "section": "show running-config interface",
                "action": "capture_interface_config",
            }
        )

        self.assertEqual(df["interface"].tolist(), ["GigabitEthernet1/0/3"])

        interface = df.iloc[0]
        self.assertEqual(interface["description"], "Link to ISP: Primary")
        self.assertEqual(interface["ipv6 address"], "2001:db8::1/64")

    def test_capture_interface_config_with_alias_and_raw_column(self) -> None:
        """Aliases and ``include_raw`` interact correctly for interface capture."""

        config_path = self._write_config(
            [
                "--- show running-config interface",
                "interface Loopback0",
                " ip address 192.0.2.1 255.255.255.255",
                " quit",
                "Some unrelated line",
                "PORT: Ethernet1",
                " description Test port",
                "ENDPORT",
            ]
        )

        ingestor = SwitchLore(config_path)

        df = ingestor.query(
            [
                {
                    "section": "show running-config interface",
                    "action": "capture_interfaces",
                    "options": {
                        "terminators": ["quit", "ENDPORT"],
                        "interface_pattern": r"^(?:interface|PORT:)\s+(.+)$",
                    },
                }
            ],
            include_raw=True,
        )

        self.assertEqual(df["interface"].tolist(), ["Loopback0", "Ethernet1"])
        self.assertIn("raw", df.columns)
        self.assertIn("interface Loopback0", df["configuration"].iloc[0])
        self.assertIn("PORT: Ethernet1", df["configuration"].iloc[1])
        self.assertEqual(df["raw"].iloc[0], df["configuration"].iloc[0])
        self.assertEqual(df["raw"].iloc[1], df["configuration"].iloc[1])
        self.assertNotEqual(df["raw"].iloc[0], df["raw"].iloc[1])

        loopback = df[df["interface"] == "Loopback0"].iloc[0]
        self.assertEqual(loopback["ip address"], "192.0.2.1 255.255.255.255")

        ethernet = df[df["interface"] == "Ethernet1"].iloc[0]
        self.assertEqual(ethernet["description"], "Test port")

    def test_query_accepts_single_mapping_specification(self) -> None:
        """A lone command mapping can be supplied without wrapping it in a list."""

        config_path = self._write_config(
            [
                "--- show running-config interface",
                "interface GigabitEthernet1/0/10",
                " description Access port",
                " switchport access vlan 20",
                " exit",
            ]
        )

        ingestor = SwitchLore(config_path)

        df = ingestor.query(
            {
                "section": "show running-config interface",
                "action": "capture_interface_config",
            },
            include_raw=True,
        )

        self.assertEqual(df["interface"].tolist(), ["GigabitEthernet1/0/10"])
        self.assertEqual(df["raw"].iloc[0], df["configuration"].iloc[0])
        self.assertIn("switchport access vlan 20", df["raw"].iloc[0])

    def test_invalid_command_mapping_raises(self) -> None:
        """Invalid command specifications raise descriptive exceptions."""

        config_path = self._write_config(
            ["--- show inventory", "Chassis"]
        )

        ingestor = SwitchLore(config_path)

        with self.assertRaises(TypeError):
            ingestor.query([{ "section": 42 }])

        with self.assertRaises(ValueError):
            ingestor.query([{"action": "capture_interface_config"}])

        with self.assertRaises(ValueError):
            ingestor.query(
                [
                    {
                        "section": "show running-config interface",
                        "action": "unknown",
                    }
                ]
            )
