#' Create Experiment
#'
#' Creates an MLflow experiment and returns its id.
#'
#' @importFrom purrr is_empty
#'
#' @param name The name of the experiment to create.
#' @param artifact_location Location where all artifacts for this experiment are stored. If
#'   not provided, the remote server will select an appropriate default.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @param tags Experiment tags to set on the experiment upon experiment creation.
#'
#' @export
create_experiment <- function(name, artifact_location, client, tags) {

  if (is_missing(name)) abort("You must specify an experiment name")

  assert_string(name)

  .args <- resolve_args(
    name = name,
    client = maybe_missing(client),
    artifact_location = maybe_missing(artifact_location),
    tags = maybe_missing(tags)
  )

  .args$tags <- .args$tags %>%
    imap(~ list(key = .y, value = .x)) %>%
    unname()

  response <- call_mlflow_api(
    "experiments", "create",
    client = .args$client,
    verb = "POST",
    data = list(
      name = .args$name,
      artifact_location = .args$artifact_location,
      tags = .args$tags
    )
  )

  response$experiment_id
}

#' List Experiments
#'
#' Gets a list of all experiments.
#'
#' @importFrom purrr reduce
#' @importFrom tibble as_tibble
#'
#' @param view_type Qualifier for type of experiments to be returned. Defaults to `ACTIVE_ONLY`.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
list_experiments <- function(view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"), client) {

  client <- resolve_client(maybe_missing(client))
  view_type <- match.arg(view_type)

  response <- call_mlflow_api(
    "experiments", "list",
    client = client,
    verb = "GET",
    query = list(
      view_type = view_type
    )
  )

  # Return `NULL` if no experiments
  if (!length(response)) {
    return(NULL)
  }

  result <- map(
    response$experiments,
    function(x) {
      x$tags <- list(parse_run_data(x$tags))
      as_tibble(x)
    }
  )

  reduce(
    result,
    rbind
  )
}

#' Set Experiment Tag
#'
#' Sets a tag on an experiment with the specified ID. Tags are experiment metadata that can be updated.
#'
#' @importFrom checkmate assert_string
#'
#' @param key Name of the tag. All storage backends are guaranteed to support
#'   key values up to 250 bytes in size. This field is required.
#' @param value String value of the tag being logged. All storage backends are
#'   guaranteed to support key values up to 5000 bytes in size. This field is required.
#' @param experiment_id ID of the experiment.
#' @param client An MLFlow client. Will be auto-generated if not specified.
#'
#' @export
set_experiment_tag <- function(key, value, experiment_id, client) {

  assert_string(key)
  assert_string(value)

  .args <- resolve_args(
    client = maybe_missing(client),
    experiment_id = maybe_missing(experiment_id)
  )

  call_mlflow_api(
    "experiments",
    "set-experiment-tag",
    client = .args$client,
    verb = "POST",
    data = list(
      experiment_id = .args$experiment_id,
      key = key,
      value = value
    )
  )

  invisible()
}

#' Get Experiment
#'
#' Gets metadata for an experiment and a list of runs for the experiment. Attempts to obtain the
#' active experiment if both `experiment_id` and `name` are unspecified.
#'
#' @param experiment_id ID of the experiment.
#' @param name The experiment name. Only one of `name` or `experiment_id` should be specified.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
get_experiment <- function(experiment_id, name, client) {

  if (!is_missing(name) && !is_missing(experiment_id)) {
    abort("Only one of `name` or `experiment_id` should be specified.")
  }

  if (!is_missing(name))  assert_string(name)

  client <- resolve_client(maybe_missing(client))
  if (!is_missing(experiment_id)) experiment_id <- resolve_experiment_id(experiment_id)

  response <- if (!is_missing(name)) {
    call_mlflow_api("experiments", "get-by-name",
      client = client,
      query = list(
        experiment_name = name
      )
    )
  } else {
    response <- call_mlflow_api(
      "experiments", "get",
      client = client,
      query = list(
        experiment_id = experiment_id
      )
    )
  }

  response$experiment$tags <- parse_run_data(response$experiment$tags)
  response$experiment %>%
    new_mlflow_experiment()
}

#' Delete Experiment
#'
#' Marks an experiment and associated runs, params, metrics, etc. for deletion. If the
#'   experiment uses FileStore, artifacts associated with experiment are also deleted.
#'
#' @param experiment_id ID of the associated experiment. This field is required.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
delete_experiment <- function(experiment_id, client) {
  .args <- resolve_args(
    client = maybe_missing(client),
    experiment_id = maybe_missing(experiment_id)
  )

  if (identical(.args$experiment_id, get_active_experiment_id())) {
    abort("Cannot delete an active experiment.")
  }

  call_mlflow_api(
    "experiments", "delete",
    verb = "POST",
    client = .args$client,
    data = list(
      experiment_id = .args$experiment_id
    )
  )
}



#' Restore Experiment
#'
#' Restores an experiment marked for deletion. This also restores associated metadata,
#'   runs, metrics, and params. If experiment uses FileStore, underlying artifacts
#'   associated with experiment are also restored.
#'
#' Throws `RESOURCE_DOES_NOT_EXIST` if the experiment was never created or was permanently deleted.
#'
#' @param experiment_id ID of the associated experiment. This field is required.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
restore_experiment <- function(experiment_id, client) {

  .args <- resolve_args(
    client = maybe_missing(client),
    experiment_id = maybe_missing(experiment_id)
  )

  call_mlflow_api(
    "experiments", "restore",
    client = .args$client,
    verb = "POST",
    data = list(
      experiment_id = .args$experiment_id
    )
  )

  invisible()
}

#' Rename Experiment
#'
#' Renames an experiment.
#'

#' @param experiment_id ID of the associated experiment. This field is required.
#' @param new_name The experiment's name will be changed to this. The new name must be unique.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
rename_experiment <- function(new_name, experiment_id, client) {

  assert_string(new_name)

  .args <- resolve_args(
    client = maybe_missing(client),
    experiment_id = maybe_missing(experiment_id)
  )

  call_mlflow_api(
    "experiments", "update",
    client = .args$client,
    verb = "POST",
    data = list(
      experiment_id = .args$experiment_id,
      new_name = new_name
    )
  )

  invisible()
}

#' Set Experiment
#'
#' Sets an experiment as the active experiment. Either the name or ID of the experiment can be provided.
#'   If the a name is provided but the experiment does not exist, this function creates an experiment
#'   with provided name. Returns the ID of the active experiment.
#'
#' @param experiment_name Name of experiment to be activated.
#' @param experiment_id ID of experiment to be activated.
#' @param artifact_location Location where all artifacts for this experiment are stored. If
#'   not provided, the remote server will select an appropriate default.
#' @export
set_experiment <- function(experiment_name, experiment_id, artifact_location) {

  if (!is_missing(experiment_name) && !is_missing(experiment_id)) {
    abort("Only one of `experiment_name` or `experiment_id` should be specified.")
  }

  if (is_missing(experiment_name) && missing(experiment_id)) {
    abort("Exactly one of `experiment_name` or `experiment_id` should be specified.")
  }

  client <- mlflow_client()

  final_experiment_id <- if (!is_missing(experiment_name)) {
    tryCatch(
      mlflow_id(get_experiment(client = client, name = assert_string(experiment_name))),
      error = function(e) {
        if (grepl(e$message, "Could not find experiment with name")) {

          inform("Experiment `", experiment_name, "` does not exist. Creating a new experiment.")
          create_experiment(
            client = client,
            name = experiment_name,
            artifact_location = artifact_location
          )
        } else {
          abort(e)
        }
      }
    )
  } else {
    assert_string(experiment_id)
  }

  invisible(set_active_experiment_id(final_experiment_id))
}
