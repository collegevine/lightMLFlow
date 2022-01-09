#' Create registered model
#'
#' Creates a new registered model in the model registry
#'
#' @param name The name of the model to create.
#' @param tags Additional metadata for the registered model (Optional).
#' @param description Description for the registered model (Optional).
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
create_registered_model <- function(name, tags, description, client) {

  stop_for_missing_args(
    name = maybe_missing(name)
  )

  assert_string(name)

  .args <- resolve_args(
    tags = maybe_missing(tags),
    description = maybe_missing(description),
    client = maybe_missing(client)
  )

  response <- tryCatch(
    {
      call_mlflow_api(
        "registered-models",
        "create",
        client = .args$client,
        verb = "POST",
        data = list(
          name = name,
          tags = .args$tags,
          description = .args$description
        )
      )
    },
    error = function(cond) {
      if (grepl("RESOURCE_ALREADY_EXISTS", cond$message)) {
        warn(
          sprintf(
            "An experiment named %s already exists.",
            name
          )
        )

        list(
          registered_model = get_registered_model(
            name = name
          )
        )
      } else {
        abort(
          cond$message,
          trace = cond$trace
        )
      }
    }
  )

  return(response$registered_model)
}

#' Get a registered model
#'
#' Retrieves a registered model from the Model Registry.
#'
#' @param name The name of the model to retrieve.
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
get_registered_model <- function(name, client) {

  stop_for_missing_args(name = maybe_missing(name))
  assert_string(name)

  client <- resolve_client(maybe_missing(client))

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
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
rename_registered_model <- function(name, new_name, client) {

  stop_for_missing_args(
    name = maybe_missing(name),
    new_name = maybe_missing(new_name)
  )

  assert_string(name)
  assert_string(new_name)

  client <- resolve_client(maybe_missing(client))

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
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
update_registered_model <- function(name, description, client) {

  stop_for_missing_args(name = maybe_missing(name))
  assert_string(name)

  .args <- resolve_args(
    client = maybe_missing(client),
    description = maybe_missing(description)
  )

  response <- call_mlflow_api(
    "registered-models",
    "update",
    client = .args$client,
    verb = "PATCH",
    data = list(
      name = name,
      description = .args$description
    )
  )

  return(response$registered_model)
}

#' Delete registered model
#'
#' Deletes an existing registered model by name
#'
#' @param name The name of the model to delete
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
delete_registered_model <- function(name, client) {

  stop_for_missing_args(name = maybe_missing(name))
  assert_string(name)

  client <- resolve_client(maybe_missing(client))

  call_mlflow_api(
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
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
list_registered_models <- function(max_results = 100, page_token = NULL, client) {
  client <- resolve_client(maybe_missing(client))

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
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
get_latest_versions <- function(name, stages, client) {

  .args <- resolve_args(
    stages = maybe_missing(stages),
    client = maybe_missing(client)
  )

  stop_for_missing_args(name = maybe_missing(name))
  assert_string(name)

  response <- call_mlflow_api(
    "registered-models",
    "get-latest-versions",
    client = .args$client,
    verb = "POST",
    data = list(
      name = name,
      stages = .args$stages
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
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
create_model_version <- function(name, source, run_id, tags, run_link, description, client) {

  stop_for_missing_args(
    name = maybe_missing(name),
    source = maybe_missing(source)
  )

  assert_string(name)
  assert_string(source)

  .args <- resolve_args(
    run_id = maybe_missing(run_id),
    tags = maybe_missing(tags),
    run_link = maybe_missing(run_link),
    description = maybe_missing(description),
    client = maybe_missing(client)
  )

  response <- call_mlflow_api(
    "model-versions",
    "create",
    client = .args$client,
    verb = "POST",
    data = list(
      name = name,
      source = source,
      run_id = .args$run_id,
      run_link = .args$run_link,
      description = .args$description
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
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
get_model_version <- function(name, version, client) {

  stop_for_missing_args(
    name = maybe_missing(name),
    version = maybe_missing(version)
  )

  assert_string(name)
  assert_string(version)

  client <- resolve_client(maybe_missing(client))

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

  response$model_version %>%
    list_modify(
      creation_timestamp = milliseconds_to_datetime(.[["creation_timestamp"]]),
      last_updated_timestamp = milliseconds_to_datetime(.[["last_updated_timestamp"]])
    )
}

#' Update model version
#'
#' Updates a model version
#'
#' @param name Name of the registered model.
#' @param version Model version number.
#' @param description Description of this model version.
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
update_model_version <- function(name, version, description, client) {

  stop_for_missing_args(
    name = maybe_missing(name),
    version = maybe_missing(version)
  )

  assert_string(name)
  assert_string(version)

  .args <- resolve_args(
    client = maybe_missing(client),
    description = maybe_missing(description)
  )

  response <- call_mlflow_api(
    "model-versions",
    "update",
    client = .args$client,
    verb = "PATCH",
    data = list(
      name = name,
      version = version,
      description = .args$description
    )
  )

  return(response$model_version)
}

#' Delete a model version
#'
#' @param name Name of the registered model.
#' @param version Model version number.
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
delete_model_version <- function(name, version, client) {

  stop_for_missing_args(
    name = maybe_missing(name),
    version = maybe_missing(version)
  )

  assert_string(name)
  assert_string(version)

  client <- resolve_client(maybe_missing(client))

  call_mlflow_api(
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
#' @importFrom checkmate assert_logical
#' @param name Name of the registered model.
#' @param version Model version number.
#' @param stage Transition `model_version` to this tage.
#' @param archive_existing_versions (Optional)
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @export
transition_model_version_stage <- function(name, version, stage, archive_existing_versions = FALSE, client) {

  stop_for_missing_args(
    name = maybe_missing(name),
    version = maybe_missing(version),
    stage = maybe_missing(stage)
  )

  assert_string(name)
  assert_string(version)
  assert_string(stage)
  assert_logical(archive_existing_versions)

  client <- resolve_client(maybe_missing(client))

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
