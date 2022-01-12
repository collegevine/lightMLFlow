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

#' @importFrom tibble tibble
#' @importFrom rlang names2
get_key_value_df <- function(...) {
  values <- list(...) %>% unlist()
  keys <- names2(values)
  args <- as.list(sys.call(-1))
  fname <- args[[1]]
  if(fname == "log_metrics") {
    values <- values %>% map_chr(metric_value_to_rest)
  }
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
#' @importFrom forge cast_string cast_scalar_double cast_nullable_scalar_double
#' @importFrom rlang maybe_missing
#' @importFrom checkmate assert_double
#' @importFrom dplyr mutate
#'
#' @export
log_metrics <- function(..., timestamp, step, run_id, client) {

  metrics <- get_key_value_df(...)

  .timestamp <- maybe_missing(timestamp, default = NA)
  .step <- maybe_missing(step, default = NA)
  metrics <- metrics %>%
    mutate(
      timestamp = .timestamp,
      step = .step
    )

  log_batch(
    metrics = metrics,
    run_id = maybe_missing(run_id),
    client = maybe_missing(client)
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
create_run <- function(start_time, tags, experiment_id, client) {

  .args <- resolve_args(
    experiment_id = maybe_missing(experiment_id),
    client = maybe_missing(client),
    start_time = maybe_missing(start_time),
    tags = maybe_missing(tags)
  )

  .args$tags <- .args$tags %>%
    imap(~ list(key = .y, value = .x)) %>%
    unname()

  data <- list(
    experiment_id = .args$experiment_id,
    start_time = .args$start_time * 1000,  ## convert to ms
    tags = .args$tags
  )

  response <- call_mlflow_api(
    "runs", "create",
    client = .args$client,
    verb = "POST",
    data = data
  )

  run_id <- response$run$info$run_id
  register_tracking_event("create_run", data)

  get_run(run_id = run_id, client = .args$client)
}

#' Delete a Run
#'
#' Deletes the run with the specified ID.
#' @export
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
delete_run <- function(run_id, client) {

  .args <- resolve_args(
    run_id = maybe_missing(run_id),
    client = maybe_missing(client)
  )

  if (identical(.args$run_id, get_active_run_id())) {
    abort("Cannot delete an active run.")
  }

  data <- list(run_id = .args$run_id)
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
restore_run <- function(run_id, client) {

  stop_for_missing_args(
    run_id = maybe_missing(run_id)
  )

  assert_string(run_id)

  client <- resolve_client(maybe_missing(client))
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
get_run <- function(run_id, client) {

  .args <- resolve_args(
    run_id = maybe_missing(run_id),
    client = maybe_missing(client)
  )

  response <- call_mlflow_api(
    "runs", "get",
    client = .args$client,
    verb = "GET",
    query = list(
      run_id = .args$run_id
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
log_batch <- function(metrics = data.frame(), params = data.frame(), tags = data.frame(), run_id, client) {

  .args <- resolve_args(
    run_id = maybe_missing(run_id),
    client = maybe_missing(client)
  )

  validate_batch_input("metrics", metrics, c("key", "value", "step", "timestamp"))
  validate_batch_input("params", params, c("key", "value"))
  validate_batch_input("tags", tags, c("key", "value"))

  metrics$value <- unlist(lapply(metrics$value, metric_value_to_rest))
  if (nrow(params) > 0) {
    params$value <- as.character(params$value)
  }

  data <- list(
    run_id = .args$run_id,
    metrics = metrics,
    params = params,
    tags = tags
  )

  call_mlflow_api(
    "runs", "log-batch",
    client = .args$client,
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
set_tag <- function(key, value, run_id, client) {

  stop_for_missing_args(
    key = maybe_missing(key),
    value = maybe_missing(value)
  )

  assert_string(key)
  assert_string(value)

  .args <- resolve_args(
    run_id = maybe_missing(run_id),
    client = maybe_missing(client)
  )

  data <- list(
    run_id = .args$run_id,
    key = key,
    value = value
  )

  call_mlflow_api(
    "runs", "set-tag",
    client = .args$client,
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
delete_tag <- function(key, run_id, client) {

  stop_for_missing_args(key = maybe_missing(key))
  assert_string(key)

  .args <- resolve_args(
    client = maybe_missing(client),
    run_id = maybe_missing(run_id)
  )

  data <- list(
    run_id = .args$run_id,
    key = key
  )

  call_mlflow_api(
    "runs", "delete-tag",
    client = .args$client,
    verb = "POST",
    data = data
  )

  register_tracking_event(
    "delete_tag",
    data
  )

  invisible()
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
log_params <- function(..., run_id, client) {

  params <- get_key_value_df(...)

  log_batch(
    params = params,
    run_id = maybe_missing(run_id),
    client = maybe_missing(client)
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
get_metric_history <- function(metric_key, run_id, client) {

  stop_for_missing_args(
    metric_key = maybe_missing(metric_key)
  )

  assert_string(metric_key)

  .args <- resolve_args(
    run_id = maybe_missing(run_id),
    client = maybe_missing(client)
  )

  response <- call_mlflow_api(
    "metrics", "get-history",
    client = .args$client,
    verb = "GET",
    query = list(
      run_id = .args$run_id,
      metric_key = metric_key
    )
  )

  if (is_empty(response$metrics)) {
    abort(
      sprintf(
        "Could not find a metric called %s in run %s.",
        metric_key,
        .args$run_id
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
#' @importFrom forge cast_string_list cast_nullable_string
#'
#' @export
search_runs <- function(filter = NULL, run_view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"), experiment_ids = NULL, order_by = list(), client = NULL) {
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
#' @param run_id A run id Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @importFrom purrr transpose
#' @importFrom rlang inform
#'
#' @export
list_artifacts <- function(path = NULL, run_id, client) {

  .args <- resolve_args(
    run_id = maybe_missing(run_id),
    client = maybe_missing(client)
  )

  response <- call_mlflow_api(
    "artifacts", "list",
    client = .args$client,
    verb = "GET",
    query = list(
      run_id = .args$run_id,
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
#' @param ... Additional arguments to pass to `aws.s3::save_object`
#' @export
download_artifact <- function(path, run_id, ...) {

  run_id <- resolve_run_id(maybe_missing(run_id))

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

#' Log Artifact
#'
#' Logs a specific file or directory as an artifact for a run. Modeled after `aws.s3::s3write_using`
#'
#' @param x The object to log as an artifact
#' @param FUN the function to use to save the artifact
#' @param filename the name of the file to save
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
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
#' @return The path to the file
#' @export
log_artifact <- function(x, FUN = saveRDS, filename, run_id, ...) {

  run_id <- resolve_run_id(maybe_missing(run_id))

  experiment_id <- get_experiment_from_run(
    run_id = run_id
  )

  s3_info <- get_s3_bucket_and_prefix()
  s3_file <- create_s3_path(
    s3_prefix = s3_info$prefix,
    experiment_id = experiment_id,
    run_id = run_id,
    fname = filename
  )

  s3write_using(
    x = x,
    FUN = FUN,
    ...,
    object = s3_file,
    bucket = s3_info$bucket
  )

  s3_file
}

#' Record logged model metadata with the tracking server.
#'
#' @param model_spec A model specification.
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @importFrom jsonlite toJSON
record_logged_model <- function(model_spec, run_id, client) {

  .args <- resolve_args(
    run_id = maybe_missing(run_id),
    client = maybe_missing(client)
  )

  call_mlflow_api(
    "runs", "log-model",
    client = .args$client,
    verb = "POST",
    data = list(
      run_id = .args$run_id,
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
start_run <- function(run_id, experiment_id, start_time, tags, client, nested = FALSE) {

  assert_logical(nested)

  # When `client` is provided, this function acts as a wrapper for `runs/create` and does not register
  #  an active run.
  if (!is_missing(client)) {
    if (!is_missing(run_id)) abort("`run_id` should not be specified when `client` is specified.")
    run <- create_run(
      client = client,
      start_time = start_time,
      tags = tags,
      experiment_id = experiment_id
    )
    return(run)
  }

  # Fluent mode, check to see if extraneous params passed.

  if (!is_missing(start_time)) abort("`start_time` should only be specified when `client` is specified.")
  if (!is_missing(tags)) abort("`tags` should only be specified when `client` is specified.")

  active_run_id <- get_active_run_id()
  if (!is.null(active_run_id) && !nested) {
    abort("Run with id ",
          active_run_id,
          " is already active. To start a nested run, Call `start_run()` with `nested = TRUE`."
    )
  }

  existing_run_id <- if (!is_missing(run_id)) {
    run_id
  } else if (Sys.getenv("MLFLOW_run_id") != "") {
    Sys.getenv("MLFLOW_run_id")
  } else {
    NULL
  }

  client <- mlflow_client()

  run <- if (!is.null(existing_run_id)) {
    # This is meant to pick up existing run when we're inside `mlflow_source()` called via `mlflow run`.
    get_run(client = client, run_id = existing_run_id)
  } else {
    experiment_id <- ifelse(
      is_missing(experiment_id),
      infer_experiment_id(),
      experiment_id
    )

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
end_run <- function(status = c("FINISHED", "FAILED", "KILLED"), end_time = NULL, run_id, client) {
  status <- match.arg(status)
  end_time <- end_time %||% (current_time() * 1000) ## convert to ms

  active_run_id <- get_active_run_id()

  if (!is_missing(client) && is_missing(run_id)) {
    abort("`run_id` must be specified when `client` is specified.")
  }

  run <- if (!is_missing(run_id)) {
    client <- resolve_client(maybe_missing(client))
    set_terminated(
      client = client,
      run_id = run_id,
      status = status,
      end_time = end_time
    )
  } else {
    if (is.null(active_run_id)) abort("There is no active run to end.")
    client <- mlflow_client()
    run_id <- active_run_id
    set_terminated(
      client = client,
      run_id = active_run_id,
      status = status,
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
