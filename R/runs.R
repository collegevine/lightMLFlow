#' @include globals.R
NULL

#' @importFrom tibble tibble
#' @importFrom rlang names2
get_key_value_df <- function(..., .which = -1) {
  values <- list(...) %>% unlist()
  keys <- names2(values)
  args <- as.list(sys.call(which = .which))
  backup_keys <- args[2:length(args)] %>%
    as.vector() %>%
    as.character() %>%
    make.names()
  values <- unname(values)
  for(i in seq_along(values)) {
    keys[i] <- ifelse(keys[i] == "", backup_keys[i], keys[i])
  }
  tibble(
    key = keys,
    value = values,
  )
}

#' @importFrom checkmate assert
assert_new_col_length <- function(x, metrics) {
  nm <- deparse(substitute(x))
  n_metrics <- nrow(metrics)
  n_x <- length(x)
  cnd <- n_x == n_metrics | n_x == 1
  if(cnd) {
    return(TRUE)
  }
  abort(
    sprintf(
      'The length of `%s` should be 1 or the same as the number of metrics (%s), not %s.',
      nm,
      n_metrics,
      n_x
    )
  )
}

#' Log Metrics
#'
#' Logs a metric for a run. Metrics key-value pair that records a single float measure.
#'   During a single execution of a run, a particular metric can be logged several times.
#'   The MLflow Backend keeps track of historical metric values along two axes: timestamp and step.
#'
#' @param ... variable names from which a data.frame with `key` and `value` columns will be created.
#' @param timestamp Timestamp at which to log the metric. Timestamp is rounded to the nearest
#'  integer. If unspecified, the number of milliseconds since the Unix epoch is used.
#' @param step Step at which to log the metric. Step is rounded to the nearest integer. If
#'  unspecified, the default value of zero is used.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#'
#' @importFrom rlang maybe_missing
#'
#' @export
log_metrics <- function(..., timestamp = NA, step = NA, run_id = get_active_run_id(), client = mlflow_client()) {

  metrics <- get_key_value_df(...)

  assert_new_col_length(timestamp, metrics)
  assert_new_col_length(step, metrics)
  assert_string(run_id)
  assert_mlflow_client(client)

  metrics$timestamp <- timestamp
  metrics$step <- step

  log_batch(
    metrics = metrics,
    run_id = run_id,
    client = client
  )
}



#' Create an MLFlow run
#'
#' @importFrom purrr imap
#'
#' @param start_time The start time of the run. Defaults to the current time.
#' @param tags Additional tags to supply for the run
#' @param experiment_id The ID of the experiment to register the run under.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
create_run <- function(start_time = current_time(), tags = list(), experiment_id = get_active_experiment_id(), client = mlflow_client()) {

  assert_integerish(start_time)
  assert_list(tags)
  assert_string(experiment_id)
  assert_mlflow_client(client)

  tags <- tags %>%
    imap(~ list(key = .y, value = .x)) %>%
    unname()

  data <- list(
    experiment_id = experiment_id,
    start_time = start_time * 1000,  ## convert to ms
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
delete_run <- function(run_id = get_active_run_id(), client = mlflow_client()) {

  assert_string(run_id)
  assert_mlflow_client(client)

  if (exists_active_run() && identical(run_id, get_active_run_id())) {
    abort("Cannot delete an active run.")
  }

  data <- list(run_id = run_id)
  call_mlflow_api(
    "runs", "delete",
    client = client,
    verb = "POST",
    data = data
  )

  register_tracking_event("delete_run", data)
  invisible()
}

#' Restore a Run
#'
#' Restores the run with the specified ID.
#' @export
#' @param run_id A run id
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
restore_run <- function(run_id = get_active_run_id(), client = mlflow_client()) {

  assert_string(run_id)
  assert_mlflow_client(client)

  data <- list(run_id = run_id)
  call_mlflow_api(
    "runs", "restore",
    client = client,
    verb = "POST",
    data = data
  )
  register_tracking_event("restore_run", data)

  get_run(run_id, client = client)
}

#' Get Run
#'
#' Gets metadata, params, tags, and metrics for a run. Returns a single value for each metric
#' key: the most recently logged metric value at the largest step.
#'
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
get_run <- function(run_id = get_active_run_id(), client = mlflow_client()) {

  assert_string(run_id)
  assert_mlflow_client(client)

  response <- call_mlflow_api(
    "runs", "get",
    client = client,
    verb = "GET",
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
#'
#' @importFrom checkmate assert_data_frame
#'
#' @param metrics A dataframe of metrics to log, containing the following columns: "key", "value",
#'  "step", "timestamp". This dataframe cannot contain any missing ('NA') entries.
#' @param params A dataframe of params to log, containing the following columns: "key", "value".
#'  This dataframe cannot contain any missing ('NA') entries.
#' @param tags A dataframe of tags to log, containing the following columns: "key", "value".
#'  This dataframe cannot contain any missing ('NA') entries.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @export
log_batch <- function(metrics = data.frame(), params = data.frame(), tags = data.frame(), run_id = get_active_run_id(), client = mlflow_client()) {

  assert_data_frame(metrics)
  assert_data_frame(params)
  assert_data_frame(tags)
  assert_string(run_id)
  assert_mlflow_client(client)

  validate_batch_input("metrics", metrics, c("key", "value", "step", "timestamp"))
  validate_batch_input("params", params, c("key", "value"))
  validate_batch_input("tags", tags, c("key", "value"))

  params$value <- unlist(lapply(params$value, param_value_to_rest))

  data <- list(
    run_id = run_id,
    metrics = metrics,
    params = params,
    tags = tags
  )

  call_mlflow_api(
    "runs", "log-batch",
    client = client,
    verb = "POST",
    data = data
  )

  register_tracking_event(
    "log_batch",
    data
  )

  invisible()
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
set_tag <- function(key, value, run_id = get_active_run_id(), client = mlflow_client()) {

  check_required(key)
  check_required(value)

  assert_string(key)
  assert_string(value)
  assert_mlflow_client(client)

  data <- list(
    run_id = run_id,
    key = key,
    value = value
  )

  call_mlflow_api(
    "runs", "set-tag",
    client = client,
    verb = "POST",
    data = data
  )

  register_tracking_event(
    "set_tag",
    data
  )

  invisible()
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
delete_tag <- function(key, run_id = get_active_run_id(), client = mlflow_client()) {

  check_required(key)
  assert_string(key)
  assert_string(run_id)
  assert_mlflow_client(client)

  data <- list(
    run_id = run_id,
    key = key
  )

  call_mlflow_api(
    "runs", "delete-tag",
    client = client,
    verb = "POST",
    data = data
  )

  register_tracking_event(
    "delete_tag",
    data
  )

  invisible()
}

## Translate param value to safe format for REST.
## Don't use case_when to avoid dplyr dep.
param_value_to_rest <- function(value) {
  ifelse(
    is.nan(value),
    "NaN",
    ifelse(
      is.infinite(value),
      ifelse(
        value < 0,
        "-Infinity",
        "Infinity"
      ),
      as.character(value)
    )
  )
}

#' Log Parameters
#'
#' Logs parameters for a run. Examples are params and hyperparams
#'   used for ML training, or constant dates and values used in an ETL pipeline.
#'   A param is a STRING key-value pair. For a run, a single parameter is allowed
#'   to be logged only once.
#'
#' @inheritParams log_metrics
#'
#' @export
log_params <- function(..., run_id = get_active_run_id(), client = mlflow_client()) {

  assert_string(run_id)
  assert_mlflow_client(client)

  params <- get_key_value_df(...)
  params$value <- param_value_to_rest(params$value)

  log_batch(
    params = params,
    run_id = run_id,
    client = client
  )
}

#' Get Metric History
#'
#' Get a list of all values for the specified metric for a given run.
#'
#' @param metric_key Name of the metric.
#'
#' @importFrom tibble as_tibble
#' @importFrom purrr list_modify
#'
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
get_metric_history <- function(metric_key, run_id = get_active_run_id(), client = mlflow_client()) {

  check_required(metric_key)
  assert_string(metric_key)
  assert_string(run_id)
  assert_mlflow_client(client)

  response <- call_mlflow_api(
    "metrics", "get-history",
    client = client,
    verb = "GET",
    query = list(
      run_id = run_id,
      metric_key = metric_key
    )
  )

  if (is_empty(response$metrics)) {
    abort(
      sprintf(
        "Could not find a metric called %s in run %s.",
        metric_key,
        run_id
      )
    )
  } else {
    response$metrics %>%
      map(
        function(.x) {
          .x %>%
            list_modify(
              timestamp = as.POSIXct(.x$timestamp, origin = '1970-01-01',tz='UTC')
            )
        }
      ) %>%
      map(as_tibble) %>%
      reduce(rbind)
  }
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
#' @export
search_runs <- function(experiment_ids, run_view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"), order_by = list(), filter = "", client = mlflow_client()) {

  check_required(experiment_ids)

  # If we get back a single experiment ID, e.g. the active experiment ID, convert it to a list
  if (is.atomic(experiment_ids)) {
    experiment_ids <- list(experiment_ids)
  }

  assert_list(experiment_ids)
  run_view_type <- match.arg(run_view_type)
  assert_list(order_by)
  assert_string(filter)
  assert_mlflow_client(client)

  response <- call_mlflow_api(
    "runs", "search",
    client = client,
    verb = "POST",
    data = list(
      experiment_ids = experiment_ids,
      filter = "",
      run_view_type = run_view_type,
      order_by = list()
    )
  )

  runs_list <- response$run %>%
    map(parse_run)

  do.call("rbind", runs_list) %||% data.frame()
}

#' Load an artifact into an R object
#'
#' @importFrom checkmate assert_function
#' @importFrom aws.s3 s3read_using
#'
#' @param artifact_name The name of the artifact to load
#' @param run_id A run ID to find the URI for
#' @param client An MLFlow client
#' @param FUN a function to use to load the artifact
#' @param \dots Additional arguments to pass on to `s3read_using`
#'
#' @return An R object. The result of `s3read_using`
#' @export
load_artifact <- function(artifact_name, FUN = readRDS, run_id = get_active_run_id(), client = mlflow_client(), ...) {

  assert_function(FUN)
  assert_string(artifact_name)
  assert_string(run_id)
  assert_mlflow_client(client)

  artifact_location <- get_artifact_path(
    run_id = run_id,
    client = client
  )

  object <- s3read_using(
    FUN = FUN,
    ...,
    object = paste(artifact_location, artifact_name, sep = "/")
  )

  object
}

#' Get the artifact path for a run
#'
#' @param run_id A run id. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Auto-generated if not provided.
#'
#' @return A path to the run's artifacts in S3
#' @export
get_artifact_path <- function(run_id = get_active_run_id(), client = mlflow_client()) {
  experiment_id <- get_experiment_from_run(run_id = run_id)

  experiment <- get_experiment(
    experiment_id = experiment_id,
    client = client
  )

  paste(
    experiment$artifact_location,
    run_id,
    "artifacts",
    sep = "/"
  )
}
#' List Artifacts
#'
#' Gets a list of artifacts.
#'
#' @param path The run's relative artifact path to list from. If not specified, it is
#'  set to the root artifact path
#' @param run_id A run id Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @importFrom purrr transpose
#' @importFrom rlang inform
#'
#' @export
list_artifacts <- function(path = NULL, run_id = get_active_run_id(), client = mlflow_client()) {

  assert_string(path, null.ok = TRUE)
  assert_string(run_id)
  assert_mlflow_client(client)

  response <- call_mlflow_api(
    "artifacts", "list",
    client = client,
    verb = "GET",
    query = list(
      run_id = run_id,
      path = path
    )
  )

  inform(sprintf("Root URI: %s", response$root_uri))

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

#' Log Artifact
#'
#' Logs a specific file or directory as an artifact for a run. Modeled after `aws.s3::s3write_using`
#'
#' @param x The object to log as an artifact
#' @param FUN the function to use to save the artifact
#' @param filename the name of the file to save
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Auto-generated if not provided
#' @param ... Additional arguments to pass to `aws.s3::s3write_using`
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
#' @importFrom aws.s3 s3write_using
#'
#' @return The path to the file, invisibly
#' @export
log_artifact <- function(x, FUN, filename, run_id, client = mlflow_client(), ...) {
  UseMethod("log_artifact")
}

#' @rdname log_artifact
#' @export
log_artifact.default <- function(x, FUN = saveRDS, filename, run_id = get_active_run_id(), client = mlflow_client(), ...) {

  check_required(x)
  check_required(filename)

  artifact_dir = get_artifact_path(
    run_id = run_id,
    client = client
  )

  artifact_filepath <- paste(artifact_dir, filename, sep = "/")

  s3write_using(
    x = x,
    FUN = FUN,
    ...,
    object = artifact_filepath
  )

  invisible(artifact_filepath)
}

#' @importFrom aws.s3 put_object
#' @importFrom tools file_ext
#' @rdname log_artifact
#' @export
log_artifact.ggplot <- function(x, FUN, filename, run_id = get_active_run_id(), client = mlflow_client(), ...) {

  check_required(x)
  check_required(FUN)
  check_required(filename)

  ## based on https://github.com/hrbrmstr/hrbrthemes/blob/master/R/aaa.r
  if (isFALSE(requireNamespace("ggplot2", quietly = TRUE))) {
    abort(
      "Package `ggplot2` required for `ggsave`.\n",
      "Please install and try again."
    )
  }

  artifact_dir = get_artifact_path(
    run_id = run_id,
    client = client
  )

  artifact_filepath <- paste(artifact_dir, filename, sep = "/")

  ext <- file_ext(artifact_filepath)
  temp_file <- tempfile(fileext = ext)
  on.exit(unlink(temp_file, recursive = TRUE))

  ggplot2::ggsave(filename = temp_file, plot = x, ...)

  put_object(
    file = temp_file,
    object = artifact_filepath
  )

  invisible(artifact_filepath)
}

#' Record logged model metadata with the tracking server.
#'
#' @param model_spec A model specification.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @importFrom jsonlite toJSON
record_logged_model <- function(model_spec, run_id = get_active_run_id(), client = mlflow_client()) {

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
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @param nested Controls whether the run to be started is nested in a parent run. `TRUE` creates a nest run.
#'
#' @export
start_run <- function(run_id = Sys.getenv("MLFLOW_RUN_ID"), experiment_id = get_active_experiment_id(), client = mlflow_client(), nested = FALSE) {

  assert_logical(nested)
  assert_string(run_id)
  assert_string(experiment_id)
  assert_mlflow_client(client)

  if (exists_active_run() && !nested) {
    abort(
      paste(
        "Run with id",
        get_active_run_id(),
        "is already active. To start a nested run, Call `start_run()` with `nested = TRUE`."
      )
    )
  }

  run <- if (run_id != "") {
    # This is meant to pick up existing run when we're inside `mlflow_source()` called via `mlflow run`.
    get_run(client = client, run_id = run_id)
  } else {
    experiment_id <- ifelse(
      is.null(experiment_id),
      infer_experiment_id(),
      experiment_id
    )

    args <- get_run_context(
      client,
      experiment_id = experiment_id
    )

    do.call(create_run, args)
  }

  push_active_run_id(mlflow_id(run))
  set_active_experiment_id(experiment_id = run$experiment_id)

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

  if (exists_active_run()) {
    # create a tag containing the parent run ID so that MLflow UI can display
    # nested runs properly
    tags[[MLFLOW_TAGS$MLFLOW_PARENT_run_id]] <- get_active_run_id()
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
#' @importFrom checkmate assert_number
#'
#' @param status Updated status of the run. Defaults to `FINISHED`. Can also be set to
#' "FAILED" or "KILLED".
#' @param end_time Unix timestamp of when the run ended in milliseconds.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @export
end_run <- function(status = c("FINISHED", "FAILED", "KILLED"), end_time = current_time() * 1000, run_id = get_active_run_id(), client = mlflow_client()) {

  status <- match.arg(status)
  assert_number(end_time)
  assert_string(run_id)
  assert_mlflow_client(client)

  run <- set_terminated(
    client = client,
    run_id = run_id,
    status = status,
    end_time = end_time
  )

  if (exists_active_run() && identical(run_id, get_active_run_id())) pop_active_run_id()

  run
}

MLFLOW_TAGS <- list(
  MLFLOW_USER = "mlflow.user",
  MLFLOW_SOURCE_NAME = "mlflow.source.name",
  MLFLOW_SOURCE_VERSION = "mlflow.source.version",
  MLFLOW_SOURCE_TYPE = "mlflow.source.type",
  MLFLOW_PARENT_run_id = "mlflow.parentRunId"
)
