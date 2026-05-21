# DLT Ingestion Framework — Examples

A collection of worked examples demonstrating different ingestion patterns using the [Lakeflow Pipeline Ingestion Framework](https://github.com/cpiazza01/lakeflow-pipeline-ingestion-framework-dabs).

Each example is self-contained: it has its own `pipeline_config.yaml` and a README explaining the pattern, when to use it, and how to deploy it.

## Examples

| Example | Pattern | Source | Description |
|---|---|---|---|
| [lakeflow-pipeline-ingestion-framework-examples](./lakeflow-pipeline-ingestion-framework-examples/) | SCD1 + Streaming | Databricks Volume (CSV) | Daily cron-triggered incremental ingestion from a Unity Catalog Volume; Volume provisioned by the bundle |

## Prerequisites

- Databricks CLI installed and configured (`databricks configure`)
- The Lakeflow Pipeline Ingestion Framework installed: `pip install git+https://github.com/cpiazza01/lakeflow-pipeline-ingestion-framework-dabs.git`
- A Unity Catalog-enabled Databricks workspace
- Target catalog and schemas created in Unity Catalog

## General Usage

Each example follows the same three-step workflow:

```bash
# 1. Customize the pipeline_config.yaml for your environment

# 2. Generate the DABs bundle artifacts
lakeflow-generate --config pipeline_config.yaml --env dev

# 3. Deploy to Databricks
databricks bundle deploy --target dev
```
