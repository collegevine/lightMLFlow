#' @include globals.R
NULL

# Translate metric to value to safe format for REST.
metric_value_to_rest <- function(value) {
  if (is.nan(value)) {
    as.character(NaN)
  } else if (value == Inf) {
    "Infinity"
  } else if (value == -Inf) {
    "-Infinity"
  } else {
    as.character(value)
  }
}

#' Log Metric
#'
#' Logs a metric for a run. Metrics key-value pair that records a single float measure.
#'   During a single execution of a run, a particular metric can be logged several times.
#'   The MLflow Backend keeps track of historical metric values along two axes: timestamp and step.
#'
#' @param key Name of the metric.
#' @param value Float value for the metric being logged.
#' @param timestamp Timestamp at which to log the metric. Timestamp is rounded to the nearest
#'  integer. If unspecified, the number of milliseconds since the Unix epoch is used.
#' @param step Step at which to log the metric. Step is rounded to the nearest integer. If
#'  unspecified, the default value of zero is used.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#'
#' @importFrom forge cast_string cast_scalar_double cast_nullable_scalar_double
#'
#' @export
log_metric <- function(key, value, timestamp = NULL, step = NULL, run_id = NULL,
                              client = NULL) {

  c_r <- resolve_client_and_run_id(client, run_id)
  client <- c_r$client
  run_id <- c_r$run_id

  key <- cast_string(key)
  value <- cast_scalar_double(value, allow_na = TRUE)

  # convert Inf to 'Infinity'
  value <- metric_value_to_rest(value)
  timestamp <- cast_nullable_scalar_double(timestamp)
  timestamp <- round(timestamp %||% current_time())
  step <- round(cast_nullable_scalar_double(step) %||% 0)

  data <- list(
    run_id = run_id,
    key = key,
    value = value,
    timestamp = timestamp,
    step = step
  )

  call_mlflow_api(
    "runs", "log-metric",
    client = client,
    verb = "POST",
    data = data
  )

  register_tracking_event("log_metric", data)

  invisible(value)
}

#' Create an MLFlow run
#'
#' @importFrom purrr imap
#'
#' @param start_time The start time of the run. Defaults to the current time.
#' @param tags Additional tags to supply for the run
#' @param experiment_id The ID of the experiment to register the run under.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
create_run <- function(start_time = NULL, tags = list(), experiment_id = NULL, client) {
  experiment_id <- resolve_experiment_id(experiment_id)

  # Read user_id from tags
  # user_id is deprecated and will be removed from a future release
  user_id <- tags[[MLFLOW_TAGS$MLFLOW_USER]] %||% "unknown"

  tags <- if (!is_empty(tags)) {
    tags %>%
      imap(~ list(key = .y, value = .x)) %>%
      unname()
  }

  start_time <- start_time %||% current_time()

  data <- list(
    experiment_id = experiment_id,
    user_id = user_id,
    start_time = start_time,
    tags = tags
  )

  response <- call_mlflow_api(
    "runs", "create",
    client = client,
    verb = "POST",
    data = data
  )

  run_id <- response$run$info$run_id
  register_tracking_event("create_run", data)

  get_run(run_id = run_id, client = client)
}

#' Delete a Run
#'
#' Deletes the run with the specified ID.


#' @export
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
delete_run <- function(run_id, client = NULL) {
  run_id <- cast_string(run_id)
  if (identical(run_id, get_active_run_id())) {
    abort("Cannot delete an active run.")
  }
  client <- resolve_client(client)
  data <- list(run_id = run_id)
  call_mlflow_api("runs", "delete", client = client, verb = "POST", data = data)
  register_tracking_event("delete_run", data)
  invisible(NULL)
}

#' Restore a Run
#'
#' Restores the run with the specified ID.


#' @export
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
restore_run <- function(run_id, client = NULL) {
  run_id <- cast_string(run_id)
  client <- resolve_client(client)
  data <- list(run_id = run_id)
  call_mlflow_api("runs", "restore", client = client, verb = "POST", data = data)
  register_tracking_event("restore_run", data)

  get_run(run_id, client = client)
}

#' Get Run
#'
#' Gets metadata, params, tags, and metrics for a run. Returns a single value for each metric
#' key: the most recently logged metric value at the largest step.
#'


#' @export
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
get_run <- function(run_id = NULL, client = NULL) {

  run_id <- resolve_run_id(run_id)
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "runs", "get",
    client = client, verb = "GET",
    query = list(
      run_id = run_id
    )
  )

  parse_run(response$run)
}

#' Log Batch
#'
#' Log a batch of metrics, params, and/or tags for a run. The server will respond with an error (non-200 status code)
#'   if any data failed to be persisted. In case of error (due to internal server error or an invalid request), partial
#'   data may be written.
#' @param metrics A dataframe of metrics to log, containing the following columns: "key", "value",
#'  "step", "timestamp". This dataframe cannot contain any missing ('NA') entries.
#' @param params A dataframe of params to log, containing the following columns: "key", "value".
#'  This dataframe cannot contain any missing ('NA') entries.
#' @param tags A dataframe of tags to log, containing the following columns: "key", "value".
#'  This dataframe cannot contain any missing ('NA') entries.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @export
log_batch <- function(metrics = data.frame(), params = data.frame(), tags = data.frame(), run_id = NULL,
                             client = NULL) {

  validate_batch_input("metrics", metrics, c("key", "value", "step", "timestamp"))
  validate_batch_input("params", params, c("key", "value"))
  validate_batch_input("tags", tags, c("key", "value"))

  metrics$value <- unlist(lapply(metrics$value, metric_value_to_rest))
  if (nrow(params) > 0) {
    params$value <- as.character(params$value)
  }

  c_r <- resolve_client_and_run_id(client, run_id)
  client <- c_r$client
  run_id <- c_r$run_id

  data <- list(
    run_id = run_id,
    metrics = metrics,
    params = params,
    tags = tags
  )

  call_mlflow_api("runs", "log-batch", client = client, verb = "POST", data = data)
  register_tracking_event("log_batch", data)

  invisible(NULL)
}

has_nas <- function(df) {
  any(is.na(df[, which(names(df) != "value")])) ||
    any(is.na(df$value) & !is.nan(df$value))
}

validate_batch_input <- function(input_type, input_dataframe, expected_column_names) {

  if (is.null(input_dataframe) || nrow(input_dataframe) == 0) {
    return()
  } else if (!setequal(names(input_dataframe), expected_column_names)) {
    msg <- paste(input_type,
      " batch input dataframe must contain exactly the following columns: ",
      paste(expected_column_names, collapse = ", "),
      ". Found: ",
      paste(names(input_dataframe), collapse = ", "),
      sep = ""
    )
    abort(msg)
  } else if (has_nas(input_dataframe)) {
    msg <- paste(input_type,
      " batch input dataframe contains a missing ('NA') entry.",
      sep = ""
    )
    abort(msg)
  }
}

#' Set Tag
#'
#' Sets a tag on a run. Tags are run metadata that can be updated during a run and
#'  after a run completes.
#'
#' @param key Name of the tag. Maximum size is 255 bytes. This field is required.
#' @param value String value of the tag being logged. Maximum size is 500 bytes. This field is required.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @export
set_tag <- function(key, value, run_id = NULL, client = NULL) {
  c_r <- resolve_client_and_run_id(client, run_id)
  client <- c_r$client
  run_id <- c_r$run_id

  key <- cast_string(key)
  value <- cast_string(value)

  data <- list(
    run_id = run_id,
    key = key,
    value = value
  )

  call_mlflow_api("runs", "set-tag", client = client, verb = "POST", data = data)
  register_tracking_event("set_tag", data)

  invisible(NULL)
}

#' Delete Tag
#'
#' Deletes a tag on a run. This is irreversible. Tags are run metadata that can be updated during a run and
#'  after a run completes.
#'
#' @param key Name of the tag. Maximum size is 255 bytes. This field is required.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
delete_tag <- function(key, run_id = NULL, client = NULL) {
  c_r <- resolve_client_and_run_id(client, run_id)
  client <- c_r$client
  run_id <- c_r$run_id

  key <- cast_string(key)

  data <- list(run_id = run_id, key = key)
  call_mlflow_api("runs", "delete-tag", client = client, verb = "POST", data = data)
  register_tracking_event("delete_tag", data)

  invisible(NULL)
}

#' Log Parameter
#'
#' Logs a parameter for a run. Examples are params and hyperparams
#'   used for ML training, or constant dates and values used in an ETL pipeline.
#'   A param is a STRING key-value pair. For a run, a single parameter is allowed
#'   to be logged only once.
#'
#' @param key Name of the parameter.
#' @param value String value of the parameter.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
log_param <- function(key, value, run_id = NULL, client = NULL) {
  c_r <- resolve_client_and_run_id(client, run_id)
  client <- c_r$client
  run_id <- c_r$run_id

  key <- cast_string(key)
  value <- cast_string(value)

  data <- list(
    run_id = run_id,
    key = key,
    value = cast_string(value)
  )

  call_mlflow_api("runs", "log-parameter", client = client, verb = "POST", data = data)
  register_tracking_event("log_param", data)

  invisible(value)
}

#' Get Metric History
#'
#' Get a list of all values for the specified metric for a given run.
#'


#' @param metric_key Name of the metric.
#'
#' @importFrom tibble as_tibble
#'
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
get_metric_history <- function(metric_key, run_id = NULL, client = NULL) {
  run_id <- resolve_run_id(run_id)
  client <- resolve_client(client)

  metric_key <- cast_string(metric_key)

  response <- call_mlflow_api(
    "metrics", "get-history",
    client = client, verb = "GET",
    query = list(run_id = run_id, run_id = run_id, metric_key = metric_key)
  )

  response$metrics %>%
    transpose() %>%
    map(unlist) %>%
    map_at("timestamp", milliseconds_to_date) %>%
    map_at("step", as.double) %>%
    as.data.frame()
}

#' Search Runs
#'
#' Search for runs that satisfy expressions. Search expressions can use Metric and Param keys.
#'
#' @param experiment_ids List of string experiment IDs (or a single string experiment ID) to search
#' over. Attempts to use active experiment if not specified.
#' @param filter A filter expression over params, metrics, and tags, allowing returning a subset of runs.
#'   The syntax is a subset of SQL which allows only ANDing together binary operations between a param/metric/tag and a constant.
#' @param run_view_type Run view type.
#' @param order_by List of properties to order by. Example: "metrics.acc DESC".
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @importFrom forge cast_string_list
#'
#' @export
search_runs <- function(filter = NULL,
                               run_view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"),
                               experiment_ids = NULL,
                               order_by = list(),
                               client = NULL) {
  experiment_ids <- resolve_experiment_id(experiment_ids)
  # If we get back a single experiment ID, e.g. the active experiment ID, convert it to a list
  if (is.atomic(experiment_ids)) {
    experiment_ids <- list(experiment_ids)
  }
  client <- resolve_client(client)

  run_view_type <- match.arg(run_view_type)
  experiment_ids <- cast_string_list(experiment_ids)
  filter <- cast_nullable_string(filter)

  response <- call_mlflow_api("runs", "search", client = client, verb = "POST", data = list(
    experiment_ids = experiment_ids,
    filter = filter,
    run_view_type = run_view_type,
    order_by = cast_string_list(order_by)
  ))

  runs_list <- response$run %>%
    map(parse_run)
  do.call("rbind", runs_list) %||% data.frame()
}

#' List Artifacts
#'
#' Gets a list of artifacts.
#'


#' @param path The run's relative artifact path to list from. If not specified, it is
#'  set to the root artifact path
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @importFrom glue glue
#'
#' @export
list_artifacts <- function(path = NULL, run_id = NULL, client = NULL) {
  run_id <- resolve_run_id(run_id)
  client <- resolve_client(client)

  response <- call_mlflow_api(
    "artifacts", "list",
    client = client, verb = "GET",
    query = list(
      run_id = run_id,
      path = path
    )
  )

  message(glue("Root URI: {uri}", uri = response$root_uri))

  files_list <- if (!is.null(response$files)) response$files else list()
  files_list <- map(files_list, function(file_info) {
    if (is.null(file_info$file_size)) {
      file_info$file_size <- NA
    }
    file_info
  })
  files_list %>%
    transpose() %>%
    map(unlist) %>%
    as.data.frame()
}

set_terminated <- function(status, end_time, run_id, client) {
  data <- list(
    run_id = run_id,
    status = status,
    end_time = end_time
  )
  response <- call_mlflow_api("runs", "update", verb = "POST", client = client, data = data)
  register_tracking_event("set_terminated", data)

  get_run(client = client, run_id = response$run_info$run_id)
}

get_experiment_from_run <- function(run_id) {
  get_run(
    run_id
  )$experiment_id %>%
    unique()
}

get_s3_bucket_and_prefix <- function(s3_uri = Sys.getenv("S3_URI")) {
  if (str_sub(s3_uri, 1, 5) != "s3://") {
    abort("Your S3_URI environment variable didn't start with 's3://'. Please set a valid URI for the environment variable and try again.\n")
  }

  s3_uri_split <- s3_uri %>%
    str_remove("s3://") %>%
    str_split("/", n = 2)

  list(
    bucket = s3_uri_split[[1]][[1]],
    prefix = s3_uri_split[[1]][[2]]
  )
}

create_s3_path <- function(s3_prefix, experiment_id, run_id, fname) {
  paste(
    s3_prefix,
    experiment_id,
    run_id,
    "artifacts",
    fname,
    sep = "/"
  )
}

#' Download Artifact
#'
#' Download an artifact file or directory from a run to a local directory if applicable,
#'   and return a local path for it.
#'
#' @importFrom rlang !! .data
#' @importFrom aws.s3 save_object
#'
#' @param path Relative source path to the desired artifact.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @param ... Additional arguments to pass to `aws.s3::save_object`
#' @export
download_artifact <- function(path, run_id = NULL, client = NULL, ...) {

  experiment_id <- get_experiment_from_run(
    run_id = run_id
  )

  s3_info <- get_s3_bucket_and_prefix()

  s3_path <- create_s3_path(
    s3_prefix = s3_info$prefix,
    experiment_id = experiment_id,
    run_id = run_id,
    fname = path
  )

  save_object(
    object = s3_path,
    bucket = s3_info$bucket,
    file = path,
    ...
  )
}

#' List Run Infos
#'
#' Returns a tibble whose columns contain run metadata (run ID, etc) for all runs under the
#' specified experiment.
#'
#' @param experiment_id Experiment ID. Attempts to use the active experiment if not specified.
#' @param run_view_type Run view type.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @export
list_run_infos <- function(run_view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"),
                                  experiment_id = NULL, client = NULL) {
  experiment_id <- resolve_experiment_id(experiment_id)
  client <- resolve_client(client)

  run_view_type <- match.arg(run_view_type)
  experiment_ids <- cast_string_list(experiment_id)

  response <- call_mlflow_api("runs", "search", client = client, verb = "POST", data = list(
    experiment_ids = experiment_ids,
    filter = NULL,
    run_view_type = run_view_type
  ))

  run_infos_list <- response$runs %>%
    map("info") %>%
    map(parse_run_info)
  do.call("rbind", run_infos_list) %||% data.frame()
}

#' Log Artifact
#'
#' Logs a specific file or directory as an artifact for a run.
#'
#' @param file The file to log as an artifact.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @param ... Additional arguments to pass to `aws.s3::put_object`
#'
#' @details
#'
#' When logging to Amazon S3, ensure that you have the s3:PutObject, s3:GetObject,
#' s3:ListBucket, and s3:GetBucketLocation permissions on your bucket.
#'
#' Additionally, at least the \code{AWS_ACCESS_KEY_ID} and \code{AWS_SECRET_ACCESS_KEY}
#' environment variables must be set to the corresponding key and secrets provided
#' by Amazon IAM.
#'
#' @importFrom fs is_file
#' @importFrom stringr str_remove str_split str_sub
#' @importFrom aws.s3 put_object
#'
#' @export
log_artifact <- function(file, run_id = NULL, client = NULL, ...) {

  r_c <- resolve_client_and_run_id(
    client,
    run_id
  )

  client <- r_c$client
  run_id <- r_c$run_id

  experiment_id <- get_experiment_from_run(
    run_id = run_id
  )

  s3_info <- get_s3_bucket_and_prefix()
  s3_file <- create_s3_path(
    s3_prefix = s3_info$prefix,
    experiment_id = experiment_id,
    run_id = run_id,
    fname = file
  )

  put_object(
    file = file,
    object = s3_file,
    bucket = s3_info$bucket,
    ...
  )

  invisible(list_artifacts(run_id = run_id, path = NULL, client = client))
}

#' Record logged model metadata with the tracking server.
#'
#' @param model_spec A model specification.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @importFrom jsonlite toJSON
record_logged_model <- function(model_spec, run_id = NULL, client = NULL) {

  c_r <- resolve_client_and_run_id(client, run_id)

  client <- c_r$client
  run_id <- c_r$run_id

  call_mlflow_api(
    "runs", "log-model",
    client = client,
    verb = "POST",
    data = list(
      run_id = run_id,
      model_json = toJSON(model_spec, auto_unbox = TRUE)
    )
  )
}

#' Start Run
#'
#' Starts a new run. If `client` is not provided, this function infers contextual information such as
#'   source name and version, and also registers the created run as the active run. If `client` is provided,
#'   no inference is done, and additional arguments such as `start_time` can be provided.
#'
#' @param run_id If specified, get the run with the specified UUID and log metrics
#'   and params under that run. The run's end time is unset and its status is set to
#'   running, but the run's other attributes remain unchanged.
#' @param experiment_id Used only when `run_id` is unspecified. ID of the experiment under
#'   which to create the current run. If unspecified, the run is created under
#'   a new experiment with a randomly generated name.
#' @param start_time Unix timestamp of when the run started in milliseconds. Only used when `client` is specified.
#' @param tags Additional metadata for run in key-value pairs. Only used when `client` is specified.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @param nested Controls whether the run to be started is nested in a parent run. `TRUE` creates a nest run.
#'
#' @export
start_run <- function(run_id = NULL, experiment_id = NULL, start_time = NULL, tags = list(), client = NULL, nested = FALSE) {

  # When `client` is provided, this function acts as a wrapper for `runs/create` and does not register
  #  an active run.
  if (!is.null(client)) {
    if (!is.null(run_id)) abort("`run_id` should not be specified when `client` is specified.")
    run <- create_run(
      client = client, start_time = start_time,
      tags = tags, experiment_id = experiment_id
    )
    return(run)
  }

  # Fluent mode, check to see if extraneous params passed.

  if (!is.null(start_time)) abort("`start_time` should only be specified when `client` is specified.")
  if (!is_empty(tags)) abort("`tags` should only be specified when `client` is specified.")

  active_run_id <- get_active_run_id()
  if (!is.null(active_run_id) && !nested) {
    abort("Run with UUID ",
      active_run_id,
      " is already active. To start a nested run, Call `start_run()` with `nested = TRUE`."
    )
  }

  existing_run_id <- run_id %||% {
    env_run_id <- Sys.getenv("MLFLOW_run_id")
    if (nchar(env_run_id)) env_run_id
  }

  client <- mlflow_client()

  run <- if (!is.null(existing_run_id)) {
    # This is meant to pick up existing run when we're inside `mlflow_source()` called via `mlflow run`.
    get_run(client = client, run_id = existing_run_id)
  } else {
    experiment_id <- infer_experiment_id(experiment_id)
    client <- mlflow_client()

    args <- get_run_context(
      client,
      experiment_id = experiment_id
    )
    do.call(create_run, args)
  }
  push_active_run_id(mlflow_id(run))
  set_experiment(experiment_id = run$experiment_id)
  run
}

get_run_context <- function(client, ...) {
  UseMethod("get_run_context")
}

get_run_context.default <- function(client, experiment_id, ...) {
  tags <- list()
  tags[[MLFLOW_TAGS$MLFLOW_USER]] <- mlflow_user()
  tags[[MLFLOW_TAGS$MLFLOW_SOURCE_NAME]] <- get_source_name()
  tags[[MLFLOW_TAGS$MLFLOW_SOURCE_VERSION]] <- get_source_version()
  tags[[MLFLOW_TAGS$MLFLOW_SOURCE_TYPE]] <- MLFLOW_SOURCE_TYPE$LOCAL
  parent_run_id <- get_active_run_id()
  if (!is.null(parent_run_id)) {
    # create a tag containing the parent run ID so that MLflow UI can display
    # nested runs properly
    tags[[MLFLOW_TAGS$MLFLOW_PARENT_run_id]] <- parent_run_id
  }
  list(
    client = client,
    tags = tags,
    experiment_id = experiment_id %||% 0,
    ...
  )
}

#' End a Run
#'
#' Terminates a run. Attempts to end the current active run if `run_id` is not specified.
#'
#' @param status Updated status of the run. Defaults to `FINISHED`. Can also be set to
#' "FAILED" or "KILLED".
#' @param end_time Unix timestamp of when the run ended in milliseconds.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
end_run <- function(status = c("FINISHED", "FAILED", "KILLED"),
                           end_time = NULL, run_id = NULL, client = NULL) {
  status <- match.arg(status)
  end_time <- end_time %||% current_time()

  active_run_id <- get_active_run_id()

  if (!is.null(client) && is.null(run_id)) {
    abort("`run_id` must be specified when `client` is specified.")
  }

  run <- if (!is.null(run_id)) {
    client <- resolve_client(client)
    set_terminated(
      client = client, run_id = run_id, status = status,
      end_time = end_time
    )
  } else {
    if (is.null(active_run_id)) abort("There is no active run to end.")
    client <- mlflow_client()
    run_id <- active_run_id
    set_terminated(
      client = client, run_id = active_run_id, status = status,
      end_time = end_time
    )
  }

  if (identical(run_id, active_run_id)) pop_active_run_id()
  run
}

MLFLOW_TAGS <- list(
  MLFLOW_USER = "mlflow.user",
  MLFLOW_SOURCE_NAME = "mlflow.source.name",
  MLFLOW_SOURCE_VERSION = "mlflow.source.version",
  MLFLOW_SOURCE_TYPE = "mlflow.source.type",
  MLFLOW_PARENT_run_id = "mlflow.parentRunId"
)
