# Configure the Azure provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0.2"
    }
    azapi = {
      source  = "azure/azapi"
      version = "~> 2.12.0"
    }
  }
  backend "local" {
    path = "../terraform-state/terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "azapi" {
}

variable "LATITUDE" {
}

variable "LONGITUDE" {
}

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
  path               = "databricks"
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

# DATA FACTORY

resource "azurerm_data_factory" "df" {
  name                = "weatherorchestrator"
  resource_group_name = azurerm_resource_group.wd.name
  location            = azurerm_resource_group.wd.location
  identity {
    type = "SystemAssigned"
  }
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
        url                = "https://api.open-meteo.com"
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
        url                = "https://air-quality-api.open-meteo.com"
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
          type         = "number"
          defaultValue = 1
        }
      }
      annotations = []
      type        = "Json"
      typeProperties = {
        location = {
          type = "HttpServerLocation"
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
          defaultValue = "pm2_5,carbon_dioxide,ozone,european_aqi"
        }
        timezone = {
          type         = "string"
          defaultValue = "Europe/Berlin"
        }
        forecast_days = {
          type         = "number"
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
          type = "HttpServerLocation"
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
}

# DATA FACTORY PIPELINES

resource "azurerm_data_factory_pipeline" "om" {
  name            = "OpenMeteoIngestionPipeline"
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
                    "type": "JsonReadSettings"
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
                    "type": "JsonReadSettings"
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
                "activity": "OpenMeteo Air Ingestion Pipeline",
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
            "jobId": "392312162766182"
        },
        "linkedServiceName": {
            "referenceName": "${azapi_resource.dbls.name}",
            "type": "LinkedServiceReference"
        }
    }
]
  JSON
}
