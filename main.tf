terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.78.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "2.45.0"
    }
    azapi = {
      source  = "Azure/azapi"
      version = "1.9.0"
    }
  }
}

provider "azurerm" {
  features {}  # This is the required features block
}

# Ressource groupe dans Azure
resource "azurerm_resource_group" "example" {
  name     = "ETL2024ResourceGroup"
  location = "westeurope"
}

# Création du compte de stockage pour le Data Lake
resource "azurerm_storage_account" "data_lake" {
  name                     = "etl2024datalake"
  resource_group_name      = azurerm_resource_group.example.name
  location                 = azurerm_resource_group.example.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# Création des conteneurs pour raw-data, staging, et transformed
resource "azurerm_storage_container" "raw_data" {
  name                  = "raw-data"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "staging" {
  name                  = "staging"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "transformed" {
  name                  = "transformed"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

# Récupération de la configuration du client Azure AD et des ID d'applications publiées
data "azuread_client_config" "current" {}
data "azuread_application_published_app_ids" "well_known" {}

# Création d'un principal de service pour Microsoft Graph
resource "azuread_service_principal" "msgraph" {
  client_id    = data.azuread_application_published_app_ids.well_known.result.MicrosoftGraph
  use_existing = true
}

# Création de l'application Azure AD
resource "azuread_application" "app" {
  display_name     = "ETL2024DatabricksApp"
  sign_in_audience = "AzureADMyOrg"
  owners           = [data.azuread_client_config.current.object_id]

  required_resource_access {
    resource_app_id = data.azuread_application_published_app_ids.well_known.result.MicrosoftGraph

    resource_access {
      id   = azuread_service_principal.msgraph.oauth2_permission_scope_ids["User.Read"]
      type = "Scope"
    }
  }
}

# Création d'un principal de service pour l'application créée
resource "azuread_service_principal" "app" {
  client_id                    = azuread_application.app.client_id
  app_role_assignment_required = false
  owners                       = [data.azuread_client_config.current.object_id]

  feature_tags {
    enterprise = true
  }
}
# Création d'un mot de passe pour l'application Azure AD
resource "azuread_application_password" "app" {
  application_id = azuread_application.app.id
  display_name   = "Generated by Terraform"
}

# Création du Key Vault avec une politique d'accès
resource "azurerm_key_vault" "example" {
  name                = "etl2024keyvault"
  location            = azurerm_resource_group.example.location
  resource_group_name = azurerm_resource_group.example.name
  tenant_id           = data.azuread_client_config.current.tenant_id
  sku_name            = "standard"

  # Politique d'accès pour le principal Terraform
  access_policy {
    tenant_id = data.azuread_client_config.current.tenant_id
    object_id = data.azuread_client_config.current.object_id

    key_permissions = [
      "Create",
      "Get",
      "Delete",
      "List",
      "Update",
      "Import",
      "Backup",
      "Restore",
      "Recover",
      "Purge",
    ]

    secret_permissions = [
      "Set",
      "Get",
      "Delete",
      "List",
      "Recover",
      "Backup",
      "Restore",
      "Purge",
    ]

    certificate_permissions = [
      "Create",
      "Delete",
      "Get",
      "List",
      "Update",
      "Import",
      "Backup",
      "Restore",
      "Recover",
      "Purge",
    ]
  }
}

# Stockage du secret du principal de service dans le Key Vault
resource "azurerm_key_vault_secret" "app_secret" {
  name         = "appSecret"
  value        = azuread_application_password.app.value
  key_vault_id = azurerm_key_vault.example.id
}


resource "azurerm_data_factory" "example" {
  name                = "etl2024datafactory"
  location            = azurerm_resource_group.example.location
  resource_group_name = azurerm_resource_group.example.name
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "example" {
  name            = "etl2024-datalake-linkedservice"
  data_factory_id = azurerm_data_factory.example.id
  url             = azurerm_storage_account.data_lake.primary_blob_endpoint
  use_managed_identity = true
}

resource "azurerm_databricks_workspace" "example" {
  name                = "etl2024-databricks"
  resource_group_name = azurerm_resource_group.example.name
  location            = azurerm_resource_group.example.location
  sku                 = "standard"  
}














