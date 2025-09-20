# SwitchLore

SwitchLore consolidates common functions I've developed across multiple tools into a single package. Its primary purpose is to ingest, parse, and analyze network switch configuration files, turning raw configs into structured, queryable data.

## Core Features
- **File Ingestion** – Read individual configuration files or entire directories.
- **Parsing Engine** – Extract key details such as interfaces, VLANs, and CDP neighbors.
- **Data Organization** – Build a structured knowledge base (per-switch) from parsed configurations.
- **Action Modules** – Run queries and operations against the data (e.g., inventorying interfaces, mapping neighbors, validating settings).

## Goal
Provide a centralized, reusable toolkit for working with network switch configurations, enabling efficient analysis, documentation, and topology mapping.
