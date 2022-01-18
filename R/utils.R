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
  experiment_name <- Sys.getenv("MLFLOW_EXPERIMENT_NAME", unset = NA)
  if (!is.na(experiment_name)) {
    get_experiment(client = client, experiment_name = experiment_name)$experiment_id
  } else {
    id <- Sys.getenv("MLFLOW_EXPERIMENT_ID", unset = NA)
    if (is.na(id)) NULL else id
  }
}

infer_experiment_id <- function() {
  experiment_id <- get_active_experiment_id() %||% get_experiment_id_from_env()
  assert_string(experiment_id, null.ok = TRUE)
}

#' A `with` wrapper for MLFlow runs
#'
#' Adds some error handling on exit
#'
#' @param data data to use for constructing an environment. For the default with method this may be an environment, a list, a data frame, or an integer as in sys.call. For within, it can be a list or a data frame.
#' @param expr expression to evaluate; particularly for within() often a “compound” expression, i.e., of the form
#'
#' {
#'  a <- somefun()
#'  b <- otherfun()
#'   .....
#'  rm(unused1, temp)
#' }
#' @param ... Arguments to be passed to future methods
#'
#' @return No return value. Called for side effects
#'
#' @method with mlflow_run
#' @export with.mlflow_run
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
      end_run(status = "FAILED")
      abort(cnd$message)
    },
    interrupt = function(cnd) end_run(status = "KILLED")
  )

  invisible()
}

current_time <- function() {
  round(
    as.numeric(
      as.POSIXlt(
        Sys.time(),
        tz = "UTC"
      )
    )
  )
}

milliseconds_to_datetime <- function(x) as.POSIXct(as.double(x) / 1000, origin = "1970-01-01", tz = "UTC")

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
  r %>%
    map_at(c("start_time", "end_time"), milliseconds_to_datetime) %>%
    fill_missing_run_cols() %>%
    as.data.frame()
}

#' @importFrom purrr reduce map
parse_metric_data <- function(d) {
  if (is.null(d) || all(is.na(d)) || is_empty(d)) {
    NULL
  } else {
    d %>%
      map(as.data.frame) %>%
      reduce(rbind) %>%
      list()
  }
}

#' @importFrom purrr map_chr set_names
parse_run_data <- function(d) {
  if (is.null(d) || all(is.na(d)) || is_empty(d)) {
    NULL
  } else {
    keys <- d %>%
      map_chr(~ .x[["key"]])

    vals <- d %>%
      map_chr(~ .x[["value"]])

    vals %>%
      as.list() %>%
      set_names(keys) %>%
      list()
  }
}

new_mlflow_experiment <- function(x) {
  dx <- as_tibble(x)
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

#' @importFrom rlang is_symbol inject
is_missing0 <- function (arg, env)  {
  is_symbol(arg) && inject(missing(!!arg), env)
}
#' @importFrom rlang caller_env
is_missing2 <- function(x) {
  if (is_missing0(substitute(x), caller_env())) {
    TRUE
  }
  FALSE
}

#' @importFrom purrr keep
stop_for_missing_args <- function(...) {

  missings <- list(...) %>%
    keep(
      ~ is_missing2(.x)
    )

  if (length(missings) > 0) {
    abort(
      sprintf(
        "Missing the following required argument(s): %s",
        paste(names(missings), collapse = ", ")
      )
    )
  }

  invisible()
}

assert_mlflow_client <- function(client) {
  assert_class(client, c("mlflow_http_client", "mlflow_client"))
}

check_required <- function(arg) {
  if (missing(arg)) {
    abort(
      sprintf(
        "You must provide a value for `%s`", deparse(substitute(arg))
      )
    )
  }
}
