#' Create Experiment
#'
#' Creates an MLflow experiment and returns its id.
#'
#' @importFrom purrr is_empty
#' @importFrom checkmate assert_class assert_list
#'
#' @param name The name of the experiment to create.
#' @param artifact_location Location where all artifacts for this experiment are stored. If
#'   not provided, the remote server will select an appropriate default.
#' @param client An MLFlow client. If not provided, the client is sourced from the `MLFLOW_TRACKING_URI` environment variable.
#' @param tags Experiment tags to set on the experiment upon experiment creation.
#'
#' @return The `experiment_id` of the newly-created experiment
#' @export
create_experiment <- function(name, artifact_location = "", client = mlflow_client(), tags = list()) {

  if (missing(name)) abort("You must specify an experiment name")

  assert_string(name)
  assert_string(artifact_location)
  assert_mlflow_client(client)
  assert_list(tags)

  tags <- tags %>%
    imap(~ list(key = .y, value = .x)) %>%
    unname()

  response <- tryCatch(
    {
      call_mlflow_api(
        "experiments", "create",
        client = client,
        verb = "POST",
        data = list(
          name = name,
          artifact_location = artifact_location,
          tags = tags
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

        get_experiment(
          experiment_name = name
        )
      } else {
        abort(
          cond$message,
          trace = cond$trace
        )
      }
    }
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
#' @inheritParams create_experiment
#'
#' @return A `data.frame` of experiments, with columns `experiment_id`, `name`, `artifact_location`, `lifecycle_stage`, and `tags`
#' @export
search_experiments <- function(view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"), client = mlflow_client()) {

  view_type <- match.arg(view_type)
  assert_mlflow_client(client)

  response <- call_mlflow_api(
    "experiments", "search",
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
    bind_rows
  )
}

#' @rdname search_experiments
#' @export
list_experiments <- function(view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"), client = mlflow_client()) {
  .Deprecated("search_experiments")
  search_artifacts(view_type = view_type, client = client)
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
#' @inheritParams create_experiment
#'
#' @return No return value. Called for side effects.
#' @export
set_experiment_tag <- function(key, value, experiment_id = get_active_experiment_id(), client = mlflow_client()) {

  assert_string(key)
  assert_string(value)
  assert_string(experiment_id)
  assert_mlflow_client(client)

  call_mlflow_api(
    "experiments",
    "set-experiment-tag",
    client = client,
    verb = "POST",
    data = list(
      experiment_id = experiment_id,
      key = key,
      value = value
    )
  )

  invisible()
}

#' Get Experiment
#'
#' Gets metadata for an experiment and a list of runs for the experiment. Attempts to obtain the
#' active experiment if both `experiment_id` and `experiment_name` are unspecified.
#'
#' @param experiment_id ID of the experiment.
#' @param experiment_name The experiment name. Only one of `experiment_name` or `experiment_id` should be specified.
#' @inheritParams create_experiment
#'
#' @return A tibble with metadata on the experiment requested.
#' @export
get_experiment <- function(experiment_id = get_active_experiment_id(), experiment_name = NULL, client = mlflow_client()) {

  if (!is.null(experiment_name) && !is.null(experiment_id)) {
    warn("Both `experiment_name` and `experiment_id` were specified. This usually happens when you call `get_experiment` with an experiment name from inside of a run. Falling back on the supplied `experiment_name`.")
  }

  assert_string(experiment_id, null.ok = TRUE)
  assert_string(experiment_name, null.ok = TRUE)
  assert_mlflow_client(client)

  response <- if (!is.null(experiment_name)) {
    call_mlflow_api("experiments", "get-by-name",
      client = client,
      query = list(
        experiment_name = experiment_name
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

#' Get Experiment ID
#'
#' Makes a call to `get_experiment` and returns just the ID.
#'
#' @param experiment_name The experiment name. This field is required.
#' @inheritParams create_experiment
#'
#' @return The `experiment_id`
#' @export
get_experiment_id <- function(experiment_name, client = mlflow_client()) {
  get_experiment(
    experiment_name = experiment_name,
    client = client
  )$experiment_id
}

#' Delete Experiment
#'
#' Marks an experiment and associated runs, params, metrics, etc. for deletion. If the
#'   experiment uses FileStore, artifacts associated with experiment are also deleted.
#'
#' @param experiment_id ID of the associated experiment. This field is required.
#' @inheritParams create_experiment
#'
#' @return No return value. Called for side effects.
#' @export
delete_experiment <- function(experiment_id, client = mlflow_client()) {

  check_required(experiment_id)

  assert_string(experiment_id)
  assert_mlflow_client(client)

  if (identical(experiment_id, get_active_experiment_id())) {
    abort("Cannot delete an active experiment.")
  }

  call_mlflow_api(
    "experiments", "delete",
    verb = "POST",
    client = client,
    data = list(
      experiment_id = experiment_id
    )
  )

  invisible()
}



#' Restore Experiment
#'
#' Restores an experiment marked for deletion. This also restores associated metadata,
#'   runs, metrics, and params. If experiment uses FileStore, underlying artifacts
#'   associated with experiment are also restored.
#'
#' Throws `RESOURCE_DOES_NOT_EXIST` if the experiment was never created or was permanently deleted.
#'
#' @inheritParams delete_experiment
#'
#' @return No return value. Called for side effects.
#' @export
restore_experiment <- function(experiment_id, client = mlflow_client()) {

  check_required(experiment_id)

  assert_string(experiment_id)
  assert_mlflow_client(client)

  call_mlflow_api(
    "experiments", "restore",
    client = client,
    verb = "POST",
    data = list(
      experiment_id = experiment_id
    )
  )

  invisible()
}

#' Rename Experiment
#'
#' Renames an experiment.
#'
#' @param new_name The experiment's name will be changed to this. The new name must be unique.
#' @inheritParams delete_experiment
#'
#' @return No return value. Called for side effects.
#' @export
rename_experiment <- function(new_name, experiment_id = get_active_experiment_id(), client = mlflow_client()) {

  check_required(new_name)

  assert_string(new_name)
  assert_string(experiment_id)
  assert_mlflow_client(client)

  call_mlflow_api(
    "experiments", "update",
    client = client,
    verb = "POST",
    data = list(
      experiment_id = experiment_id,
      new_name = new_name
    )
  )

  invisible()
}

#' Create a NODELETE tag for an experiment
#'
#' We create `NODELETE` tags for experiments that we, uh, don't want to delete.
#' `NODELETE` tags will only be set in either non-interactive sessions or by explicit request.
#'
#' @importFrom utils askYesNo
#'
#' @param experiment_id The experiment ID for which to set the NODELETE tag
#'
#' @return No return value. Called for side effects.
#' @export
create_nodelete_tag <- function(experiment_id) {
  if (!interactive() || isTRUE(askYesNo("Create NODELETE tag?"))) {
    inform(
      sprintf(
        "Creating a NODELETE tag for %s.",
        experiment_id
      )
    )

    set_experiment_tag(
      key = "NODELETE",
      value = "true",
      experiment_id = experiment_id
    )
  }

  invisible()
}

#' List experiments without `NODELETE` tags
#'
#' @importFrom purrr map_lgl walk
#'
#' @param view_type Qualifier for type of experiments to be returned. Defaults to `ACTIVE_ONLY`.
#' @param client an MLFlow client
#'
#' @return A character vector of experiment IDs without `NODELETE` flags
#' @export
list_experiments_without_nodelete <- function(view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"), client = mlflow_client()) {
  all_experiments <- list_experiments(
    view_type = view_type,
    client = client
  )

  without_nodelete <- map_lgl(
    all_experiments$tags,
    ~ !("NODELETE" %in% names(unlist(.x))) ||  unlist(.x)[["NODELETE"]] != "true"
  )

  all_experiments$experiment_id[without_nodelete]
}
