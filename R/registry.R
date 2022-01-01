#' Create registered model
#'
#' Creates a new registered model in the model registry
#'
#' @param name The name of the model to create.
#' @param tags Additional metadata for the registered model (Optional).
#' @param description Description for the registered model (Optional).
#' @export
create_registered_model <- function(name, tags = list(),
                                           description = "", client = NULL) {
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "registered-models",
    "create",
    client = client,
    verb = "POST",
    data = list(
      name = cast_string(name),
      tags = tags,
      description = description
    )
  )

  return(response$registered_model)
}

#' Get a registered model
#'
#' Retrieves a registered model from the Model Registry.
#'
#' @param name The name of the model to retrieve.
#' @export
get_registered_model <- function(name, client = NULL) {
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "registered-models",
    "get",
    client = client,
    verb = "GET",
    query = list(name = name)
  )

  return(response$registered_model)
}

#' Rename a registered model
#'
#' Renames a model in the Model Registry.
#'
#' @param name The current name of the model.
#' @param new_name The new name for the model.
#' @export
rename_registered_model <- function(name, new_name, client = NULL) {
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "registered-models",
    "rename",
    client = client,
    verb = "POST",
    data = list(
      name = name,
      new_name = new_name
    )
  )

  return(response$registered_model)
}

#' Update a registered model
#'
#' Updates a model in the Model Registry.
#'
#' @param name The name of the registered model.
#' @param description The updated description for this registered model.
#' @export
update_registered_model <- function(name, description, client = NULL) {
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "registered-models",
    "update",
    client = client,
    verb = "PATCH",
    data = list(
      name = name,
      description = description
    )
  )

  return(response$registered_model)
}

#' Delete registered model
#'
#' Deletes an existing registered model by name
#'
#' @param name The name of the model to delete
#' @export
delete_registered_model <- function(name, client = NULL) {
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "registered-models",
    "delete",
    client = client,
    verb = "DELETE",
    data = list(name = name)
  )
}

#' List registered models
#'
#' Retrieves a list of registered models.
#'
#' @param max_results Maximum number of registered models to retrieve.
#' @param page_token Pagination token to go to the next page based on a
#'   previous query.
#' @export
list_registered_models <- function(max_results = 100, page_token = NULL,
                                          client = NULL) {
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "registered-models",
    "list",
    client = client,
    verb = "GET",
    query = list(
      max_results = max_results,
      page_token = page_token
    )
  )

  return(
    list(
      registered_models = response$registered_model,
      next_page_token = response$next_page_token
    )
  )
}

#' Get latest model versions
#'
#' Retrieves a list of the latest model versions for a given model.
#'
#' @importFrom purrr list_modify
#'
#' @param name Name of the model.
#' @param stages A list of desired stages. If the input list is missing, return
#'   latest versions for ALL_STAGES.
#' @export
get_latest_versions <- function(name, stages, client = NULL) {

  client <- resolve_client(client)
  if (is_missing(stages)) stages <- list("None", "Staging", "Production", "Archived")

  response <- call_mlflow_api(
    "registered-models",
    "get-latest-versions",
    client = client,
    verb = "POST",
    data = list(
      name = name,
      stages = stages
    )
  )

  response$model_versions %>%
    map(
      ~ list_modify(
        .x,
        creation_timestamp = milliseconds_to_datetime(.x[["creation_timestamp"]]),
        last_updated_timestamp = milliseconds_to_datetime(.x[["last_updated_timestamp"]])
      )
    )
}

#' Create a model version
#'
#' @param name Register model under this name.
#' @param source URI indicating the location of the model artifacts.
#' @param run_id MLflow run ID for correlation, if `source` was generated
#'   by an experiment run in MLflow Tracking.
#' @param tags Additional metadata.
#' @param run_link MLflow run link - This is the exact link of the run that
#'   generated this model version.
#' @param description Description for model version.
#' @export
create_model_version <- function(name, source, run_id = NULL,
                                        tags = NULL, run_link = "",
                                        description = "", client = NULL) {

  c_r <- resolve_client_and_run_id(client, run_id)
  client <- c_r$client
  run_id <- c_r$run_id

  response <- call_mlflow_api(
    "model-versions",
    "create",
    client = client,
    verb = "POST",
    data = list(
      name = name,
      source = source,
      run_id = run_id,
      run_link = run_link,
      description = description
    )
  )

  response$model_version %>%
    list_modify(
      creation_timestamp = milliseconds_to_datetime(.[["creation_timestamp"]]),
      last_updated_timestamp = milliseconds_to_datetime(.[["last_updated_timestamp"]])
    )
}

#' Get a model version
#'
#' @param name Name of the registered model.
#' @param version Model version number.
#' @export
get_model_version <- function(name, version, client = NULL) {
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "model-versions",
    "get",
    client = client,
    verb = "GET",
    query = list(
      name = name,
      version = version
    )
  )

  return(response$model_version)
}

#' Update model version
#'
#' Updates a model version
#'
#' @param name Name of the registered model.
#' @param version Model version number.
#' @param description Description of this model version.
#' @export
update_model_version <- function(name, version, description,
                                        client = NULL) {
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "model-versions",
    "update",
    client = client,
    verb = "PATCH",
    data = list(
      name = name,
      version = version,
      description = description
    )
  )

  return(response$model_version)
}

#' Delete a model version
#'
#' @param name Name of the registered model.
#' @param version Model version number.
#' @export
delete_model_version <- function(name, version, client = NULL) {
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "model-versions",
    "delete",
    client = client,
    verb = "DELETE",
    data = list(
      name = name,
      version = version
    )
  )
}

#' Transition ModelVersion Stage
#'
#' Transition a model version to a different stage.
#'
#' @param name Name of the registered model.
#' @param version Model version number.
#' @param stage Transition `model_version` to this tage.
#' @param archive_existing_versions (Optional)
#' @export
transition_model_version_stage <- function(name, version, stage,
                                                  archive_existing_versions = FALSE,
                                                  client = NULL) {
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "model-versions",
    "transition-stage",
    client = client,
    verb = "POST",
    data = list(
      name = name,
      version = version,
      stage = stage,
      archive_existing_versions = archive_existing_versions
    )
  )

  return(response$model_version)
}
