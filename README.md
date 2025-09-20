# SwitchLore

SwitchLore consolidates common functions I've developed across multiple tools into a single package. Its primary purpose is to ingest, parse, and analyze network switch configuration files, turning raw configs into structured, queryable data.

## Core Features
- **File Ingestion** – Read individual configuration files or entire directories.
- **Parsing Engine** – Extract key details such as interfaces, VLANs, and CDP neighbors.
- **Data Organization** – Build a structured knowledge base (per-switch) from parsed configurations.
- **Action Modules** – Run queries and operations against the data (e.g., inventorying interfaces, mapping neighbors, validating settings).

## Querying Parsed Data

`SwitchLore` provides a high-level interface that ties configuration files to
their parsed command sections. After instantiating the class you can request one
or more commands and receive the results as a Pandas `DataFrame`.

```python
from switchlore import SwitchLore

ingestor = SwitchLore("/path/to/configs")
df = ingestor.query([
    "show mac address-table",
    "show cdp neighbors detail",
])
```

The resulting dataframe keeps track of the originating file for each parsed row
and leverages [`ntc-templates`](https://github.com/networktocode/ntc-templates)
under the hood.

## Goal
Provide a centralized, reusable toolkit for working with network switch configurations, enabling efficient analysis, documentation, and topology mapping.
