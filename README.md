# Weather Dashboard

A weather and air-quality data pipeline that ingests data from the Open-Meteo APIs, processes it through a medallion architecture on Databricks, and serves it to a Power BI dashboard.

## Pipeline

![Pipeline architecture](assets/azure-architecture.drawio.svg)

Data flows through four layers:

1. **Ingestion** — Azure Data Factory copies JSON from Open-Meteo's forecast, air-quality, and historical-forecast endpoints into a bronze directory in ADLS Gen2.
2. **Bronze** — Databricks Auto Loader ingests the raw JSON into bronze tables.
3. **Silver** — Normalizes the data into hourly measurements, metadata, and unit tables.
4. **Gold** — Produces a fact table (weather and air quality joined on time) plus time and threshold dimension tables for KPI scoring.

## Dashboard

A Power BI service principal is granted read access to all schemas. The current dashboard layout:

![Dashboard preview](assets/dashboard.pdf)

## KPI Definitions

Each KPI is scored using trapezoidal functions defined in the `gold_thresholds_dim` table.

| KPI | Optimum | Curve type | Slope ranges | Realistic range |
|---|---|---|---|---|
| **Apparent temperature** | 15–22 °C | Trapezoid | Positive slope: 10–15 °C; Negative slope: 22–26 °C | −20 °C – 45 °C |
| **Surface pressure** | 1020–1030 hPa | Half trapezoid | Positive slope: 1010–1020 hPa | 954.4 hPa – 1060.8 hPa |
| **Surface pressure slope** | +3–5 hPa/24 h | Half trapezoid | Positive slope: −5 to +3 hPa/24 h | −40 to +40 hPa/24 h; ±5 hPa/h |
| **Air quality (AQI)** | 0–25 | Half trapezoid | Negative slope: 25–100 | — |
| **Cloud cover** | 0–50 % | Half trapezoid | Negative slope: 50–90 % | — |
| **Humidity** | 40–60 % | Trapezoid | Positive slope: 30–40 %; Negative slope: 60–80 % | — |
| **Wind** | 12–19 km/h | Trapezoid | Positive slope: 0–12 km/h; Negative slope: 19–39 km/h | 0 km/h – 118 km/h |

## Project Structure

```
terraform/                       # Azure & Databricks infrastructure (IaC)
databricks/transformations/      # PySpark pipeline source code
  bronze/                        #   Auto Loader ingestion
  silver/                        #   Normalization and unit extraction
  gold/                          #   Fact and dimension tables
  _columns.py                    #   Shared column names and helpers
databricks/tests/                # pytest unit tests for transformations
assets/                          # Dashboard and architecture diagrams
```

## Tech Stack

- **Cloud**: Azure (Germany West Central)
- **Compute**: Databricks (serverless, Photon)
- **Orchestration**: Azure Data Factory
- **Storage**: ADLS Gen2 with Unity Catalog
- **Infrastructure**: Terraform
- **Consumption**: Power BI
- **Data source**: Open-Meteo APIs
- **Testing**: pytest, pyright
