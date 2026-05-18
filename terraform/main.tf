terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
  }
  # Store state in Azure Blob — replace with your storage account
  backend "azurerm" {
    resource_group_name  = "rg-tfstate"
    storage_account_name = "sttfstate"
    container_name       = "tfstate"
    key                  = "nyc-taxi-pipeline.tfstate"
  }
}

provider "azurerm" {
  features {}
}

# ── Variables ────────────────────────────────────────────────────────────────

variable "location" {
  default = "East US 2"
}

variable "env" {
  default = "dev"
}

# ── Resource Group ────────────────────────────────────────────────────────────

resource "azurerm_resource_group" "main" {
  name     = "rg-nyctaxi-${var.env}"
  location = var.location
}

# ── Event Hubs ────────────────────────────────────────────────────────────────

resource "azurerm_eventhub_namespace" "main" {
  name                = "evhns-nyctaxi-${var.env}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 1
}

resource "azurerm_eventhub" "taxi" {
  name                = "taxi-events"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 1
}

resource "azurerm_eventhub_consumer_group" "stream_analytics" {
  name                = "stream-analytics"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.taxi.name
  resource_group_name = azurerm_resource_group.main.name
}

resource "azurerm_eventhub_consumer_group" "surge_alerts" {
  name                = "surge-alerts"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.taxi.name
  resource_group_name = azurerm_resource_group.main.name
}

# ── ADLS Gen2 ────────────────────────────────────────────────────────────────

resource "azurerm_storage_account" "datalake" {
  name                     = "stnyctaxi${var.env}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # ADLS Gen2 hierarchical namespace
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.datalake.id
}

# ── Stream Analytics Job ──────────────────────────────────────────────────────

resource "azurerm_stream_analytics_job" "taxi" {
  name                                     = "asa-nyctaxi-${var.env}"
  resource_group_name                      = azurerm_resource_group.main.name
  location                                 = azurerm_resource_group.main.location
  compatibility_level                      = "1.2"
  data_locale                              = "en-US"
  events_late_arrival_max_delay_in_seconds = 60
  events_out_of_order_max_delay_in_seconds = 50
  events_out_of_order_policy               = "Adjust"
  output_error_policy                      = "Drop"
  streaming_units                          = 3

  transformation_query = file("${path.module}/../stream_analytics/queries.sql")
}

# ── Azure Function App ────────────────────────────────────────────────────────

resource "azurerm_service_plan" "func" {
  name                = "asp-nyctaxi-${var.env}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  os_type             = "Linux"
  sku_name            = "Y1" # Consumption plan
}

resource "azurerm_linux_function_app" "surge_alerts" {
  name                       = "func-surge-${var.env}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  storage_account_name       = azurerm_storage_account.datalake.name
  storage_account_access_key = azurerm_storage_account.datalake.primary_access_key
  service_plan_id            = azurerm_service_plan.func.id

  site_config {
    application_stack {
      python_version = "3.11"
    }
  }

  app_settings = {
    "EVENT_HUB_NAME"         = azurerm_eventhub.taxi.name
    "EventHubConnection"     = azurerm_eventhub_namespace.main.default_primary_connection_string
    "FUNCTIONS_WORKER_RUNTIME" = "python"
  }

  identity {
    type = "SystemAssigned"
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "event_hub_namespace" {
  value = azurerm_eventhub_namespace.main.name
}

output "datalake_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "stream_analytics_job_name" {
  value = azurerm_stream_analytics_job.taxi.name
}

output "function_app_name" {
  value = azurerm_linux_function_app.surge_alerts.name
}
