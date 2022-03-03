## for cmd check
utils::globalVariables(c("."))

.globals <- new.env(parent = emptyenv())

push_active_run_id <- function(run_id) {
  .globals$active_run_stack <- c(.globals$active_run_stack, run_id)
}

pop_active_run_id <- function() {
  .globals$active_run_stack <- .globals$active_run_stack[1:length(.globals$active_run_stack) - 1]
}

#' Get the ID of the active run
#'
#' @return The run ID
#' @export
get_active_run_id <- function() {
  if (length(.globals$active_run_stack) == 0) {
    abort("No active run. Hint: Maybe either supply a `run_id` or start a run with `start_run`?")
  } else {
    .globals$active_run_stack[length(.globals$active_run_stack)]
  }
}

exists_active_run <- function() {
  tryCatch(
    {
      get_active_run_id()
      return(TRUE)
    },
    error = function(e) return(FALSE)
  )
}

#' Set an experiment to `active`
#'
#' @param experiment_id The ID of the experiment to activate
#'
#' @return No return value. Called for side effects.
#' @export
set_active_experiment_id <- function(experiment_id) {
  .globals$active_experiment_id <- experiment_id

  invisible()
}

#' Get the ID of the active experiment
#'
#' @return The active experiment ID
#' @export
get_active_experiment_id <- function() {
  .globals$active_experiment_id
}

#' Set Remote Tracking URI
#'
#' Specifies the URI to the remote MLflow server that will be used
#' to track experiments.
#'
#' @param uri The URI to the remote MLflow server.
#'
#' @export
set_tracking_uri <- function(uri) {
  .globals$tracking_uri <- uri

  invisible(uri)
}

#' Get Remote Tracking URI
#'
#' Gets the remote tracking URI. If no global is specified, defaults to the `MLFLOW_TRACKING_URI` environment variable.
#'
#' @export
get_tracking_uri <- function() {
  .globals$tracking_uri %||% Sys.getenv("MLFLOW_TRACKING_URI")
}
