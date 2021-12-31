mlflow_relative_paths <- function(paths) {
  gsub(paste0("^", file.path(getwd(), "")), "", paths)
}

get_executing_file_name <- function() {
  pattern <- "^--file="
  v <- grep(pattern, commandArgs(), value = TRUE)
  file_name <- gsub(pattern, "", v)
  if (length(file_name)) file_name
}

get_source_name <- function() {
  get_executing_file_name() %||% "<console>"
}

#' @importFrom git2r repository commits
get_source_version <- function() {
  file_name <- get_executing_file_name()
  tryCatch(
    error = function(cnd) NULL,
    {
      repo <- repository(file_name, discover = TRUE)
      commit <- commits(repo, n = 1)
      commit[[1]]@sha
    }
  )
}

get_active_run_id_or_start_run <- function() {
  get_active_run_id() %||% mlflow_id(start_run())
}


get_experiment_id_from_env <- function(client = mlflow_client()) {
  name <- Sys.getenv("MLFLOW_EXPERIMENT_NAME", unset = NA)
  if (!is.na(name)) {
    get_experiment(client = client, name = name)$experiment_id
  } else {
    id <- Sys.getenv("MLFLOW_EXPERIMENT_ID", unset = NA)
    if (is.na(id)) NULL else id
  }
}

infer_experiment_id <- function(experiment_id = NULL) {
  experiment_id %||% get_active_experiment_id() %||% get_experiment_id_from_env()
}

#' @export
with.mlflow_run <- function(data, expr, ...) {
  run_id <- mlflow_id(data)
  if (!identical(run_id, get_active_run_id())) {
    abort("`with()` should only be used with `start_run()`.")
  }

  tryCatch(
    {
      force(expr)
      end_run()
    },
    error = function(cnd) {
      message(cnd)
      end_run(status = "FAILED")
    },
    interrupt = function(cnd) end_run(status = "KILLED")
  )

  invisible(NULL)
}

current_time <- function() {
  round(as.numeric(Sys.time()) * 1000)
}

milliseconds_to_date <- function(x) as.POSIXct(as.double(x) / 1000, origin = "1970-01-01")

tidy_run_info <- function(run_info) {
  df <- as.data.frame(run_info, stringsAsFactors = FALSE)
  df$start_time <- milliseconds_to_date(df$start_time %||% NA)
  df$end_time <- milliseconds_to_date(df$end_time %||% NA)
  df
}

wait_for <- function(f, wait, sleep) {
  command_start <- Sys.time()

  success <- FALSE
  while (!success && Sys.time() < command_start + wait) {
    success <- suppressWarnings({
      tryCatch(
        {
          f()
          TRUE
        },
        error = function(err) {
          FALSE
        }
      )
    })

    if (!success) Sys.sleep(sleep)
  }

  if (!success) {
    abort("Operation failed after waiting for ", wait, " seconds")
  }
}

mlflow_user <- function() {
  if ("user" %in% names(Sys.info())) {
    Sys.info()[["user"]]
  } else {
    "unknown"
  }
}

MLFLOW_SOURCE_TYPE <- list(
  NOTEBOOK = "NOTEBOOK",
  JOB = "JOB",
  PROJECT = "PROJECT",
  LOCAL = "LOCAL",
  UNKNOWN = "UNKNOWN"
)

#' @importFrom forge cast_nullable_string
resolve_client_and_run_id <- function(client, run_id) {
  run_id <- cast_nullable_string(run_id)
  if (is.null(client)) {
    if (is.null(run_id)) {
      run_id <- get_active_run_id_or_start_run()
    }
    client <- mlflow_client()
  } else {
    client <- resolve_client(client)
    if (is.null(run_id)) abort("`run_id` must be specified when `client` is specified.")
  }
  list(client = client, run_id = run_id)
}

parse_run <- function(r) {
  info <- parse_run_info(r$info)

  info$metrics <- parse_metric_data(r$data$metrics)
  info$params <- parse_run_data(r$data$params)
  info$tags <- parse_run_data(r$data$tags)

  new_mlflow_run(info)
}

fill_missing_run_cols <- function(r) {
  # Ensure the current runs list has at least all the names in expected_list
  expected_names <- c(
    "run_id", "experiment_id", "user_id", "status", "start_time",
    "artifact_uri", "lifecycle_stage", "run_id", "end_time"
  )
  r[setdiff(expected_names, names(r))] <- NA
  r
}

#' @importFrom purrr map_at
parse_run_info <- function(r) {
  # TODO: Consider adding dplyr back after 1.0 along with a minimum rlang version to avoid
  # dependency conflicts. The dplyr implementation is likely faster.
  r %>%
    map_at(c("start_time", "end_time"), milliseconds_to_date) %>%
    fill_missing_run_cols() %>%
    as.data.frame()
}

#' @importFrom purrr transpose map
parse_metric_data <- function(d) {
  if (is.null(d)) {
    return(NA)
  }
  d %>%
    transpose() %>%
    map(unlist) %>%
    map_at("timestamp", milliseconds_to_date) %>%
    map_at("step", as.double) %>%
    map_at("value", as.double) %>%
    as.data.frame() %>%
    list()
}

parse_run_data <- function(d) {
  if (is.null(d) || all(is.na(d)) || is_empty(d)) {
    NA
  } else {
    d %>%
      transpose() %>%
      map(unlist) %>%
      as.data.frame() %>%
      list()
  }
}

resolve_experiment_id <- function(experiment_id) {
  infer_experiment_id(experiment_id) %||%
    abort("`experiment_id` must be specified when there is no active experiment.")
}

resolve_run_id <- function(run_id) {
  cast_nullable_string(run_id) %||%
    get_active_run_id() %||%
    abort("`run_id` must be specified when there is no active run.")
}

new_mlflow_experiment <- function(x) {
  dx <- as.data.frame(x)
  class(dx) <- c("mlflow_experiment", class(dx))

  dx
}

new_mlflow_run <- function(x) {
  dx <- as.data.frame(x)
  class(dx) <- c("mlflow_run", class(dx))

  dx
}


#' Get Run or Experiment ID
#'
#' Extracts the ID of the run or experiment.
#'
#' @param object An `mlflow_run` or `mlflow_experiment` object.
#' @export
mlflow_id <- function(object) {
  UseMethod("mlflow_id")
}

#' @rdname mlflow_id
#' @export
mlflow_id.mlflow_run <- function(object) {
  object$run_id %||% abort("Cannot extract Run ID.")
}

#' @rdname mlflow_id
#' @export
mlflow_id.mlflow_experiment <- function(object) {
  object$experiment_id %||% abort("Cannot extract Experiment ID.")
}

resolve_client <- function(client) {
  if (is.null(client)) {
    mlflow_client()
  } else {
    if (!inherits(client, "mlflow_client")) abort("`client` must be an `mlflow_client` object.")
    client
  }
}