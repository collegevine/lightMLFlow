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
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @param tags Experiment tags to set on the experiment upon experiment creation.
#'
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
          experiment_name = experiment_name
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
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
list_experiments <- function(view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"), client = mlflow_client()) {

  view_type <- match.arg(view_type)
  assert_mlflow_client(client)

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
#' active experiment if both `experiment_id` and `name` are unspecified.
#'
#' @param experiment_id ID of the experiment.
#' @param experiment_name The experiment name. Only one of `name` or `experiment_id` should be specified.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
get_experiment <- function(experiment_id = get_active_experiment_id(), experiment_name = NULL, client = mlflow_client()) {

  if (!is.null(experiment_name) && !is.null(experiment_id)) {
    abort("Only one of `name` or `experiment_id` should be specified.")
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

#' Delete Experiment
#'
#' Marks an experiment and associated runs, params, metrics, etc. for deletion. If the
#'   experiment uses FileStore, artifacts associated with experiment are also deleted.
#'
#' @param experiment_id ID of the associated experiment. This field is required.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
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

#' @param experiment_id ID of the associated experiment. This field is required.
#' @param new_name The experiment's name will be changed to this. The new name must be unique.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
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
