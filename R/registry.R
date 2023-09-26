#' Create registered model
#'
#' Creates a new registered model in the model registry
#'
#' @param name The name of the model to create.
#' @param tags Additional metadata for the registered model (Optional).
#' @param description Description for the registered model (Optional).
#' @param client An MLFlow client. Will be auto-generated if omitted.
#'
#' @return A list of metadata on the registered model, including the `name` and the `creation_timestamp` and `last_updated_timestamp`
#' @export
create_registered_model <- function(name, tags = list(), description = "", client = mlflow_client()) {

  check_required(name)

  assert_string(name)
  assert_list(tags)
  assert_string(description)
  assert_mlflow_client(client)

  response <- tryCatch(
    {
      call_mlflow_api(
        "registered-models",
        "create",
        client = client,
        verb = "POST",
        data = list(
          name = name,
          tags = tags,
          description = description
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
#'
#' @return Metadata on the registered model, including the `name` and the `creation_timestamp` and `last_updated_timestamp`
#' @export
get_registered_model <- function(name, client = mlflow_client()) {

  check_required(name)
  assert_string(name)
  assert_mlflow_client(client)

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
#'
#' @return Metadata on the registered model, including the `name` and the `creation_timestamp` and `last_updated_timestamp`
#'
#' @export
rename_registered_model <- function(name, new_name, client = mlflow_client()) {

  check_required(name)
  check_required(new_name)

  assert_string(name)
  assert_string(new_name)
  assert_mlflow_client(client)

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
#'
#' @return Metadata on the registered model, including the `name` and the `creation_timestamp` and `last_updated_timestamp`
#'
#' @export
update_registered_model <- function(name, description = "", client = mlflow_client()) {

  check_required(name)
  assert_string(name)
  assert_string(description)
  assert_mlflow_client(client)

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
#' @param client An MLFlow client. Will be auto-generated if omitted.
#'
#' @return No return value. Called for side effects.
#'
#' @export
delete_registered_model <- function(name, client = mlflow_client()) {

  check_required(name)
  assert_string(name)
  assert_mlflow_client(client)

  call_mlflow_api(
    "registered-models",
    "delete",
    client = client,
    verb = "DELETE",
    data = list(name = name)
  )

  invisible()
}

#' List registered models
#'
#' Retrieves a list of registered models.
#'
#' @importFrom checkmate assert_integerish
#'
#' @param max_results Maximum number of registered models to retrieve.
#' @param page_token Pagination token to go to the next page based on a
#'   previous query.
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @param parse Logical indicating whether to return the `registered_models` element
#' as a list (FALSE) or tibble (TRUE)
#'
#' @return A list of registered models and metadata on them.
#'
#' @export
list_registered_models <- function(max_results = 100, page_token = NULL, client = mlflow_client(), parse = FALSE) {

  assert_integerish(max_results)
  assert_mlflow_client(client)

  response <- call_mlflow_api(
    "registered-models",
    "search",
    client = client,
    verb = "GET",
    query = list(
      max_results = max_results,
      page_token = page_token
    )
  )

  res <- list(
    registered_models = response$registered_model,
    next_page_token = response$next_page_token
  )
  if(isFALSE(parse)) {
    return(res)
  }
  res$registered_models <- parse_registered_models(res$registered_models)
  res
}

parse_versions <- function(versions) {
  if(is.null(versions)) {
    return(tibble())
  }
  res <- tibble(
    name = map_chr(versions, "name"),
    version = map_chr(versions, "version"),
    creation_timestamp = map_chr(versions, "creation_timestamp") %>% milliseconds_to_datetime(),
    last_updated_timestamp = map_chr(versions, "last_updated_timestamp") %>% milliseconds_to_datetime(),
    current_stage = map_chr(versions, "current_stage"),
    description = map_chr(versions, "description"),
    source = map_chr(versions, "source"),
    run_id = map_chr(versions, "run_id"),
    status = map_chr(versions, "status"),
    run_link = map_chr(versions, "run_link")
  )
}

#' @importFrom purrr transpose
parse_registered_models <- function(registered_models) {

  registered_models_t <- transpose(registered_models)
  ul <- function(x) unlist(registered_models_t[[x]])
  parent <- tibble(
    name = ul("name"),
    creation_timestamp = ul("creation_timestamp") %>% milliseconds_to_datetime(),
    last_updated_timestamp = ul("last_updated_timestamp") %>% milliseconds_to_datetime()
  )
  versions <- map(registered_models, "latest_versions")

  parsed_versions <- versions %>% map(parse_versions)

  cbind(
    parent,
    tibble(latest_versions = parsed_versions)
  ) %>%
    as_tibble()
}

validate_mlflow_stage <- function(stage = c("Production", "Staging", "Archived"), ...) {
  match.arg(stage, ...)
}

#' Get a registered model run
#'
#' @param model_name A model name.
#' @param client An MLFlow client. Will be auto-generated if omitted.
#' @param stage A model stage. Set to `NULL` or `NA` to return all results.
#' @importFrom purrr pluck
#'
#' @return Run metadata for the provided registered model and stage
#' @export
get_registered_model_run <- function(model_name, client = mlflow_client(), stage = "Production") {

  versions <- get_registered_model(name = model_name, client = client)
  latest_versions <- versions %>% pluck("latest_versions")

  if(is.null(latest_versions)) {
    abort(
      sprintf("No registered models for %s.", model_name)
    )
  }

  parsed_versions <- latest_versions %>% parse_versions()
  if(isFALSE(is.null(stage)) || isFALSE(is.na(stage))) {
    validate_mlflow_stage(stage)
    parsed_versions <- parsed_versions[parsed_versions$current_stage == stage, ]
  }

  n_versions <- nrow(parsed_versions)
  if(n_versions > 1) {
    warn(
      sprintf("Returning more than 1 `run_id` (%s).", n_versions)
    )
  }

  if(n_versions == 0) {
    abort(
      sprintf('No registered models found for `stage = "%s".', stage)
    )
  }
  parsed_versions
}

#' Get a registered model run id
#'
#' @inheritParams get_registered_model_run
#' @seealso get_registered_model_run
#'
#' @return The `run_id` of the registered model of the stage provided.
#' @export
get_registered_model_run_id <- function(model_name, client = mlflow_client(), stage = "Production") {
  get_registered_model_run(
    model_name = model_name,
    client = client,
    stage = stage
  )$run_id
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
#'
#' @return A list of metadata on the latest versions of a registered model.
#' @export
get_latest_versions <- function(name, stages = list("None", "Archived", "Staging", "Production"), client = mlflow_client()) {

  check_required(name)
  assert_string(name)
  assert_list(stages)
  assert_mlflow_client(client)

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
#' @param source URI indicating the location of the model artifacts. Tries to default to the artifact URI of the active run. If no run is active, this will error.
#' @param run_id MLflow run ID for correlation, if `source` was generated
#'   by an experiment run in MLflow Tracking.
#' @param tags Additional metadata.
#' @param run_link MLflow run link - This is the exact link of the run that
#'   generated this model version.
#' @param description Description for model version.
#' @param client An MLFlow client. Will be auto-generated if omitted.
#'
#' @return A list of metadata on the newly-created model version.
#' @export
create_model_version <- function(name, source = get_run()$artifact_uri, run_id = get_active_run_id(), tags = list(), run_link = "", description = "", client = mlflow_client()) {

  check_required(name)
  check_required(source)

  assert_string(name)
  assert_string(source)
  assert_string(run_id)
  assert_list(tags)
  assert_string(run_link)
  assert_string(description)
  assert_mlflow_client(client)

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
#' @param client An MLFlow client. Will be auto-generated if omitted.
#'
#' @return A list of metadata on the model version.
#'
#' @export
get_model_version <- function(name, version, client = mlflow_client()) {

  check_required(name)
  check_required(version)

  assert_string(name)
  assert_string(version)
  assert_mlflow_client(client)

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
#'
#' @return A list of metadata on the newly-created model version.
#'
#' @export
update_model_version <- function(name, version, description = "", client = mlflow_client()) {

  check_required(name)
  check_required(version)

  assert_string(name)
  assert_string(version)
  assert_mlflow_client(client)

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
#' @param client An MLFlow client. Will be auto-generated if omitted.
#'
#' @return No return value. Called for side effects.
#'
#' @export
delete_model_version <- function(name, version, client = mlflow_client()) {

  check_required(name)
  check_required(version)

  assert_string(name)
  assert_string(version)
  assert_mlflow_client(client)

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

  invisible()
}

#' Transition Model Version Stage
#'
#' Transition a model version to a different stage.
#'
#' @importFrom checkmate assert_logical
#' @param name Name of the registered model.
#' @param version Model version number.
#' @param stage Transition `model_version` to this tage.
#' @param archive_existing_versions (Optional)
#' @param client An MLFlow client. Will be auto-generated if omitted.
#'
#' @return A list of metadata on the model version.
#'
#' @export
transition_model_version_stage <- function(name, version, stage, archive_existing_versions = FALSE, client = mlflow_client()) {

  check_required(name)
  check_required(version)
  check_required(stage)

  assert_string(name)
  assert_string(version)
  assert_string(stage)
  assert_logical(archive_existing_versions)
  assert_mlflow_client(client)

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

#' Gather metadata for a model version
#'
#' Model versions are tied to experiments, runs, and registered model names.
#' This helper returns metadata on the experiment and run that are associated with the registered model name and version provided.
#'
#' @param registered_model_name The name of the registered model
#' @param version The version of the model for which to gather metadata
#' @param client An MLFlow client. Autogenerated if not provided
#'
#' @return A list containing the `registered_model_name`, the `experiment_id`, the `experiment_name`, the `run_id`, and the `model_version`
#' @export
gather_model_metadata <- function(registered_model_name, version, client = mlflow_client()) {
  model_version_meta <- get_model_version(
    name = registered_model_name,
    version = version,
    client = client
  )

  experiment_id <- get_experiment_from_run(run_id = model_version_meta$run_id)
  experiment_name <- get_experiment(experiment_id = experiment_id, client = client)$name
  model_version <- model_version_meta$version
  run_id <- model_version_meta$run_id
  stage <- model_version_meta$current_stage

  list(
    registered_model_name = registered_model_name,
    experiment_id = experiment_id,
    experiment_name = experiment_name,
    run_id = run_id,
    model_version = model_version,
    stage = stage
  )
}

#' Gather metadata for a model version
#'
#' Model versions are tied to experiments, runs, and registered model names.
#' This helper returns metadata on the experiment and run that are associated with the Production version of the registered model provided.
#'
#' @param registered_model_name The name of the registered model
#' @param client An MLFlow client. Autogenerated if not provided
#'
#' @return A list containing the `registered_model_name`, the `experiment_id`, the `experiment_name`, the `run_id`, and the `model_version`
#' @export
gather_production_model_metadata <- function(registered_model_name, client = mlflow_client()) {
  model_version_meta <- get_latest_versions(
    name = registered_model_name,
    stages = list("Production"),
    client = client
  ) %>%
    pluck(1)

  if (is.null(model_version_meta)) {
    abort(
      sprintf(
        "No production version found for model `%s`",
        registered_model_name
      )
    )
  }

  gather_model_metadata(
    registered_model_name = registered_model_name,
    version = model_version_meta$version,
    client = client
  )
}
