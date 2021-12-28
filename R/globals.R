.globals <- new.env(parent = emptyenv())

#' @include observer.R
NULL

push_active_run_id <- function(run_id) {
  .globals$active_run_stack <- c(.globals$active_run_stack, run_id)
  register_tracking_event("active_run_id", list(run_id = run_id))
}

pop_active_run_id <- function() {
  .globals$active_run_stack <- .globals$active_run_stack[1:length(.globals$active_run_stack) - 1]
}

get_active_run_id <- function() {
  if (length(.globals$active_run_stack) == 0) {
    NULL
  } else {
    .globals$active_run_stack[length(.globals$active_run_stack)]
  }
}

set_active_experiment_id <- function(experiment_id) {
  .globals$active_experiment_id <- experiment_id
  register_tracking_event(
    "active_experiment_id", list(experiment_id = experiment_id)
  )
}

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
  register_tracking_event("tracking_uri", list(uri = uri))

  invisible(uri)
}

#' Get Remote Tracking URI
#'
#' Gets the remote tracking URI.
#'
#' @importFrom fs path_abs
#'
#' @export
get_tracking_uri <- function() {
  .globals$tracking_uri %||% {
    env_uri <- Sys.getenv("MLFLOW_TRACKING_URI")
    if (nchar(env_uri)) env_uri else paste("file://", path_abs("mlruns"), sep = "")
  }
}
