# Configure the Azure provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.2.0"
    }
    azapi = {
      source  = "azure/azapi"
      version = "~> 2.12.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.128.0"
    }
  }
  backend "azurerm" {
    resource_group_name  = "WeatherDashboard"
    storage_account_name = "weatherdatalake"
    container_name       = "tfstate"
    key                  = "weather.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "azapi" {}

provider "databricks" {
  host                        = "https://${azurerm_databricks_workspace.ws.workspace_url}"
  azure_workspace_resource_id = azurerm_databricks_workspace.ws.id
  auth_type                   = var.databricks_auth_type
}

variable "LATITUDE" {}

variable "LONGITUDE" {}

variable "databricks_auth_type" {
  default = "azure-cli"
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "wd" {
  name     = "WeatherDashboard"
  location = "germanywestcentral"
}

# DATA LAKE

resource "azurerm_storage_account" "dl" {
  name                     = "weatherdatalake"
  resource_group_name      = azurerm_resource_group.wd.name
  location                 = azurerm_resource_group.wd.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_data_lake_gen2_filesystem" "state" {
  name               = "tfstate"
  storage_account_id = azurerm_storage_account.dl.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "fs" {
  name               = "main"
  storage_account_id = azurerm_storage_account.dl.id
}

resource "azurerm_storage_data_lake_gen2_path" "bronze" {
  path               = "bronze"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.fs.name
  storage_account_id = azurerm_storage_account.dl.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "databricks" {
  path               = "catalog"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.fs.name
  storage_account_id = azurerm_storage_account.dl.id
  resource           = "directory"
}

# DATABRICKS WORKSPACE

resource "azurerm_databricks_workspace" "ws" {
  name                = "weatherworkspace"
  resource_group_name = azurerm_resource_group.wd.name
  location            = azurerm_resource_group.wd.location
  sku                 = "trial"
  custom_parameters {
    no_public_ip             = false
    storage_account_sku_name = "Standard_LRS"
  }
}

# DATABRICKS UNITY CATALOG

resource "azurerm_databricks_access_connector" "ac" {
  name                = "DataBricksAccessConnector"
  resource_group_name = azurerm_resource_group.wd.name
  location            = azurerm_resource_group.wd.location

  identity {
    type = "SystemAssigned"
  }
}

resource "databricks_storage_credential" "sc" {
  name = "adlcredential"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.ac.id
  }
  comment = "Managed identity credential managed by TF"
}

resource "databricks_external_location" "dl" {
  name            = "AzureDataLake"
  url             = "abfss://${azurerm_storage_data_lake_gen2_filesystem.fs.name}@${azurerm_storage_account.dl.name}.dfs.core.windows.net"
  credential_name = databricks_storage_credential.sc.id
  comment         = "Managed by TF"
  depends_on      = [azurerm_role_assignment.ac_dl_contributor]
}

resource "databricks_catalog" "weathercatalog" {
  name         = "weather"
  comment      = "Catalog for the weather dashboard medallion architecture."
  storage_root = "${databricks_external_location.dl.url}catalog"
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.weathercatalog.name
  name         = "bronze"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.weathercatalog.name
  name         = "silver"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.weathercatalog.name
  name         = "gold"
}

# DATABRICKS SERVICE PRINCIPAL FOR POWER BI

resource "databricks_service_principal" "powerbi" {
  display_name          = "PowerBI Service Principal"
  workspace_access      = true
  databricks_sql_access = true
}

resource "databricks_grants" "powerbi_catalog" {
  catalog = databricks_catalog.weathercatalog.name

  grant {
    principal  = databricks_service_principal.powerbi.application_id
    privileges = ["USE CATALOG"]
  }
}

resource "databricks_grants" "powerbi_bronze" {
  schema = "${databricks_catalog.weathercatalog.name}.${databricks_schema.bronze.name}"

  grant {
    principal  = databricks_service_principal.powerbi.application_id
    privileges = ["USE SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "powerbi_silver" {
  schema = "${databricks_catalog.weathercatalog.name}.${databricks_schema.silver.name}"

  grant {
    principal  = databricks_service_principal.powerbi.application_id
    privileges = ["USE SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "powerbi_gold" {
  schema = "${databricks_catalog.weathercatalog.name}.${databricks_schema.gold.name}"

  grant {
    principal  = databricks_service_principal.powerbi.application_id
    privileges = ["USE SCHEMA", "SELECT"]
  }
}

# DATABRICKS PIPELINE SOURCE FILES

locals {
  transformation_files = fileset("${path.module}/../databricks/transformations", "**/*.py")
}

resource "databricks_workspace_file" "transformations" {
  for_each = local.transformation_files

  path   = "/Shared/weather_pipeline/transformations/${each.value}"
  source = "${path.module}/../databricks/transformations/${each.value}"
}

# DATABRICKS PIPELINE

resource "databricks_pipeline" "weather_pipeline" {
  name       = "Weather pipeline"
  catalog    = databricks_catalog.weathercatalog.name
  schema     = databricks_schema.silver.name
  photon     = true
  serverless = true
  root_path  = "/Shared/weather_pipeline"

  configuration = {
    bronze_base_path                   = "abfss://${azurerm_storage_data_lake_gen2_filesystem.fs.name}@${azurerm_storage_account.dl.name}.dfs.core.windows.net/bronze/"
    "pipelines.maxFlowRetryAttempts"   = 0
    "pipelines.numUpdateRetryAttempts" = 0
    "pipelines.numStreamRetryAttempts" = 0
  }

  library {
    glob {
      include = "/Shared/weather_pipeline/transformations/**"
    }
  }

  depends_on = [databricks_workspace_file.transformations]
}

# DATABRICKS JOB

resource "databricks_job" "weather_job" {
  name = "OpenMeteo API Job"

  task {
    task_key    = "openmeteoapi"
    max_retries = 0
    pipeline_task {
      pipeline_id = databricks_pipeline.weather_pipeline.id
    }
  }

  queue {
    enabled = true
  }

  performance_target = "PERFORMANCE_OPTIMIZED"
}

# DATA FACTORY

resource "azurerm_data_factory" "df" {
  name                = "weatherorchestrator"
  resource_group_name = azurerm_resource_group.wd.name
  location            = azurerm_resource_group.wd.location
  identity {
    type = "SystemAssigned"
  }
}

# RBAC

resource "azurerm_role_assignment" "df_db_contributor" {
  scope                = azurerm_databricks_workspace.ws.id
  role_definition_name = "Contributor"
  principal_id         = azurerm_data_factory.df.identity[0].principal_id
}

resource "azurerm_role_assignment" "ac_dl_contributor" {
  scope                = azurerm_storage_account.dl.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.ac.identity[0].principal_id
}

resource "azurerm_role_assignment" "df_dl_contributor" {
  scope                = azurerm_storage_account.dl.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.df.identity[0].principal_id
}

# DATA FACTORY LINKED SERVICES

resource "azapi_resource" "dbls" {
  name      = "DatabricksLinkedService"
  type      = "Microsoft.DataFactory/factories/linkedservices@2018-06-01"
  parent_id = azurerm_data_factory.df.id

  body = {
    properties = {
      type = "AzureDatabricks"
      typeProperties = {
        domain              = "https://${azurerm_databricks_workspace.ws.workspace_url}"
        workspaceResourceId = "${azurerm_databricks_workspace.ws.id}"
        authentication      = "MSI"
      }
    }
  }
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "dl" {
  name                 = "DataLakeLinkedService"
  data_factory_id      = azurerm_data_factory.df.id
  use_managed_identity = true
  url                  = azurerm_storage_account.dl.primary_dfs_endpoint
}

resource "azapi_resource" "fc" {
  name      = "OpenMeteoWeatherLinkedService"
  type      = "Microsoft.DataFactory/factories/linkedservices@2018-06-01"
  parent_id = azurerm_data_factory.df.id

  body = {
    properties = {
      type = "HttpServer"
      typeProperties = {
        url                = "https://api.open-meteo.com/"
        authenticationType = "Anonymous"
      }
    }
  }
}

resource "azapi_resource" "aq" {
  name      = "OpenMeteoAirQualityLinkedService"
  type      = "Microsoft.DataFactory/factories/linkedservices@2018-06-01"
  parent_id = azurerm_data_factory.df.id

  body = {
    properties = {
      type = "HttpServer"
      typeProperties = {
        url                = "https://air-quality-api.open-meteo.com/"
        authenticationType = "Anonymous"
      }
    }
  }
}

resource "azapi_resource" "fchist" {
  name      = "OpenMeteoHistoricalForecastLinkedService"
  type      = "Microsoft.DataFactory/factories/linkedservices@2018-06-01"
  parent_id = azurerm_data_factory.df.id

  body = {
    properties = {
      type = "HttpServer"
      typeProperties = {
        url                = "https://historical-forecast-api.open-meteo.com/"
        authenticationType = "Anonymous"
      }
    }
  }
}

# DATA FACTORY DATASETS

resource "azapi_resource" "srcfc" {
  name      = "OpenMeteoWeatherSource"
  type      = "Microsoft.DataFactory/factories/datasets@2018-06-01"
  parent_id = azurerm_data_factory.df.id
  body = {
    properties = {
      linkedServiceName = {
        referenceName = "${azapi_resource.fc.name}"
        type          = "LinkedServiceReference"
      }
      parameters = {
        latitude = {
          type         = "string"
          defaultValue = var.LATITUDE
        }
        longitude = {
          type         = "string"
          defaultValue = var.LONGITUDE
        }
        hourly = {
          type         = "string"
          defaultValue = "temperature_2m,relative_humidity_2m,apparent_temperature,wind_speed_10m,surface_pressure,cloud_cover,uv_index"
        }
        timezone = {
          type         = "string"
          defaultValue = "Europe/Berlin"
        }
        forecast_days = {
          type         = "string"
          defaultValue = 1
        }
      }
      annotations = []
      type        = "Json"
      typeProperties = {
        location = {
          type        = "HttpServerLocation"
          relativeUrl = "v1/forecast?latitude=@{dataset().latitude}&longitude=@{dataset().longitude}&timezone=@{dataset().timezone}&forecast_days=@{dataset().forecast_days}&hourly=@{dataset().hourly}"
        }
      }
      schema = {}
    }
  }
}

resource "azapi_resource" "srcaq" {
  name      = "OpenMeteoAirQualitySource"
  type      = "Microsoft.DataFactory/factories/datasets@2018-06-01"
  parent_id = azurerm_data_factory.df.id
  body = {
    properties = {
      linkedServiceName = {
        referenceName = "${azapi_resource.aq.name}"
        type          = "LinkedServiceReference"
      }
      parameters = {
        latitude = {
          type         = "string"
          defaultValue = var.LATITUDE
        }
        longitude = {
          type         = "string"
          defaultValue = var.LONGITUDE
        }
        hourly = {
          type         = "string"
          defaultValue = "pm2_5,ozone,european_aqi"
        }
        timezone = {
          type         = "string"
          defaultValue = "Europe/Berlin"
        }
        forecast_days = {
          type         = "string"
          defaultValue = 1
        }
        domains = {
          type         = "string"
          defaultValue = "cams_europe"
        }
      }
      annotations = []
      type        = "Json"
      typeProperties = {
        location = {
          type        = "HttpServerLocation"
          relativeUrl = "v1/air-quality?latitude=@{dataset().latitude}&longitude=@{dataset().longitude}&hourly=@{dataset().hourly}&timezone=@{dataset().timezone}&forecast_days=@{dataset().forecast_days}&domains=@{dataset().domains}"
        }
      }
      schema = {}
    }
  }
}

resource "azurerm_data_factory_dataset_json" "snkfc" {
  name                = "OpenMeteoWeatherSink"
  data_factory_id     = azurerm_data_factory.df.id
  linked_service_name = azurerm_data_factory_linked_service_data_lake_storage_gen2.dl.name
  azure_blob_storage_location {
    container                = "main"
    path                     = "bronze"
    filename                 = "weather_@{utcNow('yyyy-MM-dd_HH-mm-ss')}.json"
    dynamic_filename_enabled = true
  }
  encoding = "UTF-8"
}

resource "azurerm_data_factory_dataset_json" "snkaq" {
  name                = "OpenMeteoAirQualitySink"
  data_factory_id     = azurerm_data_factory.df.id
  linked_service_name = azurerm_data_factory_linked_service_data_lake_storage_gen2.dl.name
  azure_blob_storage_location {
    container                = "main"
    path                     = "bronze"
    filename                 = "aq_@{utcNow('yyyy-MM-dd_HH-mm-ss')}.json"
    dynamic_filename_enabled = true
  }
  encoding = "UTF-8"
}

# DATA FACTORY PIPELINES

resource "azurerm_data_factory_pipeline" "om" {
  name            = "openmeteoingestionpipeline"
  data_factory_id = azurerm_data_factory.df.id
  activities_json = <<JSON
  [
            {
                "name": "OpenMeteo Weather Ingestion Pipeline",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "JsonSource",
                        "storeSettings": {
                            "type": "HttpReadSettings",
                            "requestMethod": "GET"
                        },
                        "formatSettings": {
                            "type": "JsonReadSettings",
                            "compressionProperties": null
                        }
                    },
                    "sink": {
                        "type": "JsonSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings"
                        },
                        "formatSettings": {
                            "type": "JsonWriteSettings"
                        }
                    },
                    "enableStaging": false
                },
                "inputs": [
                    {
                        "referenceName": "${azapi_resource.srcfc.name}",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "${azurerm_data_factory_dataset_json.snkfc.name}",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "OpenMeteo Air Quality Ingestion Pipeline",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "JsonSource",
                        "storeSettings": {
                            "type": "HttpReadSettings",
                            "requestMethod": "GET"
                        },
                        "formatSettings": {
                            "type": "JsonReadSettings",
                            "compressionProperties": null
                        }
                    },
                    "sink": {
                        "type": "JsonSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings"
                        },
                        "formatSettings": {
                            "type": "JsonWriteSettings"
                        }
                    },
                    "enableStaging": false
                },
                "inputs": [
                    {
                        "referenceName": "${azapi_resource.srcaq.name}",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "${azurerm_data_factory_dataset_json.snkaq.name}",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "ETL",
                "type": "DatabricksJob",
                "dependsOn": [
                    {
                        "activity": "OpenMeteo Weather Ingestion Pipeline",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    },
                    {
                        "activity": "OpenMeteo Air Quality Ingestion Pipeline",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "jobId": "${databricks_job.weather_job.id}"
                },
                "linkedServiceName": {
                    "referenceName": "${azapi_resource.dbls.name}",
                    "type": "LinkedServiceReference"
                }
            }
]
  JSON
}

# DATA FACTORY SCHEDULE TRIGGER (daily)

resource "azurerm_data_factory_trigger_schedule" "daily" {
  name            = "WeatherDailyTrigger"
  data_factory_id = azurerm_data_factory.df.id
  pipeline_name   = azurerm_data_factory_pipeline.om.name
  interval        = 1
  frequency       = "Day"
  start_time      = "2026-08-31T00:05:00Z"
  schedule {
    minutes = [5]
    hours   = [0]
  }
}

# DATA FACTORY BACKFILL SOURCE DATASETS
# Reuse the historical forecast / air-quality endpoints which return the same
# JSON shape as the forecast endpoints, but go back to a given start_date.
# start_date/end_date are mutually exclusive with forecast_days, so it is
# omitted here.

resource "azapi_resource" "srchistfc" {
  name      = "OpenMeteoWeatherBackfillSource"
  type      = "Microsoft.DataFactory/factories/datasets@2018-06-01"
  parent_id = azurerm_data_factory.df.id
  body = {
    properties = {
      linkedServiceName = {
        referenceName = "${azapi_resource.fchist.name}"
        type          = "LinkedServiceReference"
      }
      parameters = {
        latitude = {
          type         = "string"
          defaultValue = var.LATITUDE
        }
        longitude = {
          type         = "string"
          defaultValue = var.LONGITUDE
        }
        hourly = {
          type         = "string"
          defaultValue = "temperature_2m,relative_humidity_2m,apparent_temperature,wind_speed_10m,surface_pressure,cloud_cover,uv_index"
        }
        timezone = {
          type         = "string"
          defaultValue = "Europe/Berlin"
        }
        start_date = {
          type = "string"
        }
        end_date = {
          type = "string"
        }
      }
      annotations = []
      type        = "Json"
      typeProperties = {
        location = {
          type        = "HttpServerLocation"
          relativeUrl = "v1/forecast?latitude=@{dataset().latitude}&longitude=@{dataset().longitude}&timezone=@{dataset().timezone}&start_date=@{dataset().start_date}&end_date=@{dataset().end_date}&hourly=@{dataset().hourly}"
        }
      }
      schema = {}
    }
  }
}

resource "azapi_resource" "srchistaq" {
  name      = "OpenMeteoAirQualityBackfillSource"
  type      = "Microsoft.DataFactory/factories/datasets@2018-06-01"
  parent_id = azurerm_data_factory.df.id
  body = {
    properties = {
      linkedServiceName = {
        referenceName = "${azapi_resource.aq.name}"
        type          = "LinkedServiceReference"
      }
      parameters = {
        latitude = {
          type         = "string"
          defaultValue = var.LATITUDE
        }
        longitude = {
          type         = "string"
          defaultValue = var.LONGITUDE
        }
        hourly = {
          type         = "string"
          defaultValue = "pm2_5,ozone,european_aqi"
        }
        timezone = {
          type         = "string"
          defaultValue = "Europe/Berlin"
        }
        domains = {
          type         = "string"
          defaultValue = "cams_europe"
        }
        start_date = {
          type = "string"
        }
        end_date = {
          type = "string"
        }
      }
      annotations = []
      type        = "Json"
      typeProperties = {
        location = {
          type        = "HttpServerLocation"
          relativeUrl = "v1/air-quality?latitude=@{dataset().latitude}&longitude=@{dataset().longitude}&hourly=@{dataset().hourly}&timezone=@{dataset().timezone}&start_date=@{dataset().start_date}&end_date=@{dataset().end_date}&domains=@{dataset().domains}"
        }
      }
      schema = {}
    }
  }
}

# DATA FACTORY BACKFILL PIPELINE
# Parameterized with start_date/end_date so a single manual trigger run can
# backfill an arbitrary date range (e.g. 2026-05-01 .. today). Writes to the
# same bronze sink files, which the Auto Loader bronze tables pick up on the
# next pipeline run.

resource "azurerm_data_factory_pipeline" "om_bf" {
  name            = "openmeteobackfillpipeline"
  data_factory_id = azurerm_data_factory.df.id

  parameters = {
    start_date = "string"
    end_date   = "string"
  }

  activities_json = <<JSON
  [
            {
                "name": "OpenMeteo Weather Backfill",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "JsonSource",
                        "storeSettings": {
                            "type": "HttpReadSettings",
                            "requestMethod": "GET"
                        },
                        "formatSettings": {
                            "type": "JsonReadSettings",
                            "compressionProperties": null
                        }
                    },
                    "sink": {
                        "type": "JsonSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings"
                        },
                        "formatSettings": {
                            "type": "JsonWriteSettings"
                        }
                    },
                    "enableStaging": false
                },
                "inputs": [
                    {
                        "referenceName": "${azapi_resource.srchistfc.name}",
                        "type": "DatasetReference",
                        "parameters": {
                            "start_date": {
                                "value": "@pipeline().parameters.start_date",
                                "type": "Expression"
                            },
                            "end_date": {
                                "value": "@pipeline().parameters.end_date",
                                "type": "Expression"
                            }
                        }
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "${azurerm_data_factory_dataset_json.snkfc.name}",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "OpenMeteo Air Quality Backfill",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "JsonSource",
                        "storeSettings": {
                            "type": "HttpReadSettings",
                            "requestMethod": "GET"
                        },
                        "formatSettings": {
                            "type": "JsonReadSettings",
                            "compressionProperties": null
                        }
                    },
                    "sink": {
                        "type": "JsonSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings"
                        },
                        "formatSettings": {
                            "type": "JsonWriteSettings"
                        }
                    },
                    "enableStaging": false
                },
                "inputs": [
                    {
                        "referenceName": "${azapi_resource.srchistaq.name}",
                        "type": "DatasetReference",
                        "parameters": {
                            "start_date": {
                                "value": "@pipeline().parameters.start_date",
                                "type": "Expression"
                            },
                            "end_date": {
                                "value": "@pipeline().parameters.end_date",
                                "type": "Expression"
                            }
                        }
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "${azurerm_data_factory_dataset_json.snkaq.name}",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "ETL",
                "type": "DatabricksJob",
                "dependsOn": [
                    {
                        "activity": "OpenMeteo Weather Backfill",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    },
                    {
                        "activity": "OpenMeteo Air Quality Backfill",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "jobId": "${databricks_job.weather_job.id}"
                },
                "linkedServiceName": {
                    "referenceName": "${azapi_resource.dbls.name}",
                    "type": "LinkedServiceReference"
                }
            }
]
  JSON
}
