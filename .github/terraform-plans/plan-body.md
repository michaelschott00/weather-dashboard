## Terraform Plan

Automated plan generated for the latest commit on `main`.

- **Commit:** 80f00ea7473743bfc45ccd7da44e62a2c6358529
- **Run:** [33425258524](https://github.com/michaelschott00/weather-dashboard/actions/runs/33425258524)

### Plan Details

```hcl

Terraform used the selected providers to generate the following execution
plan. Resource actions are indicated with the following symbols:
  + create
  ~ update in-place

Terraform will perform the following actions:

  # azurerm_storage_container.state will be created
  + resource "azurerm_storage_container" "state" {
      + container_access_type             = "private"
      + default_encryption_scope          = (known after apply)
      + encryption_scope_override_enabled = true
      + has_immutability_policy           = (known after apply)
      + has_legal_hold                    = (known after apply)
      + id                                = (known after apply)
      + metadata                          = (known after apply)
      + name                              = "tfstate"
      + storage_account_id                = "/subscriptions/fbdcc566-737f-4934-bf8a-b239cce4097a/resourceGroups/WeatherDashboard/providers/Microsoft.Storage/storageAccounts/weatherdatalake"
      + url                               = (known after apply)
    }

  # databricks_grants.powerbi_catalog will be updated in-place
  ~ resource "databricks_grants" "powerbi_catalog" {
        id      = "catalog/weather"
        # (1 unchanged attribute hidden)

      - grant {
          - principal  = "5112ebd5-88b6-48c7-bd35-5871b00896eb" -> null
          - privileges = [
              - "USE_CATALOG",
            ] -> null
        }
      - grant {
          - principal  = "a7e5962b-564a-45f3-90ee-8cc6beaea5f3" -> null
          - privileges = [
              - "ALL_PRIVILEGES",
              - "EXTERNAL_USE_SCHEMA",
              - "MANAGE",
            ] -> null
        }
      + grant {
          + principal  = "5112ebd5-88b6-48c7-bd35-5871b00896eb"
          + privileges = [
              + "USE CATALOG",
            ]
        }

        # (1 unchanged block hidden)
    }

Plan: 1 to add, 1 to change, 0 to destroy.
```

Merge this PR to apply the changes.
