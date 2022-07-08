#' @importFrom stringr str_extract
generate_s3_key_bucket_ext <- function(artifact_name, run_id = get_active_run_id(), client = mlflow_client()) {
  artifact_location <- get_artifact_path(
    run_id = run_id,
    client = client
  )

  without_s3_prefix <- str_remove(
    artifact_location,
    "s3://"
  )

  bucket <- str_extract(
    without_s3_prefix,
    ".+?(?=/)"
  )

  path <- without_s3_prefix %>%
    str_remove(
      ".+?(?=/)"
    ) %>%
    str_sub(start = 2L)

  key <- paste(
    path, artifact_name, sep = "/"
  )

  ext <- paste0(".", file_ext(key))

  list(
    bucket = bucket,
    key = key,
    ext = ext
  )
}

#' Load an artifact into an R object
#'
#' @importFrom checkmate assert_function
#'
#' @param artifact_name The name of the artifact to load
#' @param run_id A run ID to find the URI for
#' @param client An MLFlow client
#' @param FUN a function to use to load the artifact
#' @param \dots Additional arguments to pass on to `s3read_using`
#' @param pause_base,max_times,pause_cap See \link[purrr]{insistently}
#'
#' @return An R object. The result of `s3read_using`
#' @export
load_artifact <- function(artifact_name, FUN = readRDS, run_id = get_active_run_id(), client = mlflow_client(), pause_base = .5, max_times = 5, pause_cap = 60, ...) {

  assert_function(FUN)
  assert_string(artifact_name)
  assert_string(run_id)
  assert_mlflow_client(client)

  s3_path_info <- generate_s3_key_bucket_ext(
    artifact_name = artifact_name,
    run_id = run_id,
    client = client
  )

  s3 <- s3()

  tmp <- tempfile(fileext = s3_path_info$ext)
  on.exit(unlink(tmp, recursive = TRUE))

  rate <- rate_backoff(
    pause_base = pause_base,
    max_times = max_times,
    pause_cap = pause_cap
  )

  insistently_read <- insistently(
    s3$download_file,
    rate = rate,
    quiet = FALSE
  )

  insistently_read(
    Bucket = s3_path_info$bucket,
    Key = s3_path_info$key,
    Filename = tmp
  )

  FUN(
    tmp,
    ...
  )
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
#' @return A `data.frame` of the artifacts at the path provided for the run provided.
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

#' Log Artifact
#'
#' Logs a specific file or directory as an artifact for a run. Modeled after `aws.s3::s3write_using`
#'
#' @param x The object to log as an artifact
#' @param FUN the function to use to save the artifact
#' @param filename the name of the file to save
#' @param run_id A run uuid. Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Auto-generated if not provided
#' @param pause_base,max_times,pause_cap See \link[purrr]{insistently}
#' @param ... Additional arguments to pass to `FUN`
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
#' @importFrom stringr str_remove str_split str_sub
#' @importFrom purrr insistently rate_backoff
#'
#' @return The path to the file, invisibly
#' @export
log_artifact <- function(x, FUN, filename, run_id, client = mlflow_client(), pause_base = .5, max_times = 5, pause_cap = 60, ...) {
  UseMethod("log_artifact")
}

#' @rdname log_artifact
#' @export
log_artifact.default <- function(x, FUN = saveRDS, filename, run_id = get_active_run_id(), client = mlflow_client(), pause_base = .5, max_times = 5, pause_cap = 60, ...) {

  check_required(x)
  check_required(filename)

  s3_key_bucket_ext <- generate_s3_key_bucket_ext(filename, run_id, client)

  tmp <- tempfile(fileext = s3_key_bucket_ext$ext)
  on.exit(unlink(tmp, recursive = TRUE))

  FUN(
    x,
    tmp,
    ...
  )

  s3 <- s3()

  rate <- rate_backoff(
    pause_base = pause_base,
    max_times = max_times,
    pause_cap = pause_cap
  )

  insistently_write <- insistently(
    s3$put_object,
    rate = rate,
    quiet = FALSE
  )

  insistently_write(
    Bucket = s3_key_bucket_ext$bucket,
    Key = s3_key_bucket_ext$key,
    Body = tmp
  )

  invisible(paste("s3:/", s3_key_bucket_ext$bucket, s3_key_bucket_ext$key, sep = "/"))
}

#' @importFrom tools file_ext
#' @importFrom paws.storage s3
#' @rdname log_artifact
#' @export
log_artifact.ggplot <- function(x, FUN, filename, run_id = get_active_run_id(), client = mlflow_client(), pause_base = .5, max_times = 5, pause_cap = 60, ...) {

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

  s3_key_bucket_ext <- generate_s3_key_bucket_ext(
    artifact_name = filename,
    run_id = run_id,
    client = client
  )

  temp_file <- tempfile(fileext = s3_key_bucket_ext$ext)
  on.exit(unlink(temp_file, recursive = TRUE))

  ggplot2::ggsave(filename = temp_file, plot = x, ...)

  s3 <- s3()

  rate <- rate_backoff(
    pause_base = pause_base,
    max_times = max_times,
    pause_cap = pause_cap
  )

  insistently_put <- insistently(
    s3$put_object,
    rate = rate,
    quiet = FALSE
  )

  insistently_put(
    Bucket = s3_key_bucket_ext$bucket,
    Key = s3_key_bucket_ext$key,
    Body = temp_file
  )

  invisible(paste("s3:/", s3_key_bucket_ext$bucket, s3_key_bucket_ext$key, sep = "/"))
}

#' Call the S3 SELECT API on a CSV artifact
#'
#' @param artifact_name The name of the artifact to `SELECT` from
#' @param run_id A run id Automatically inferred if a run is currently active.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#' @param pause_base,max_times,pause_cap See \link[purrr]{insistently}
#' @param Expression,ExpressionType,InputSerialization,OutputSerialization See \link[paws.storage]{s3_select_object_content}
#' @param \dots Additional arguments to pass to \link[paws.storage]{s3_select_object_content}
#'
#' @importFrom utils read.csv
#'
#' @return A data.frame of the query result
#' @export
s3_select_from_artifact <- function(
    artifact_name,
    run_id = get_active_run_id(),
    client = mlflow_client(),
    pause_base = .5,
    max_times = 5,
    pause_cap = 60,
    Expression,
    ExpressionType = "SQL",
    InputSerialization = list(CSV = list(FileHeaderInfo = "NONE", RecordDelimiter = "\n", FieldDelimiter = ","), CompressionType = "NONE"),
    OutputSerialization = list(CSV = list(RecordDelimiter = "\n", FieldDelimiter = ",", QuoteCharacter = '"', QuoteFields = "ASNEEDED")),
    ...
) {

  assert_string(artifact_name)
  assert_string(run_id)
  assert_mlflow_client(client)

  s3_bucket_key_ext <- generate_s3_key_bucket_ext(
    artifact_name = artifact_name,
    run_id = run_id,
    client = client
  )

  s3 <- s3()

  rate <- rate_backoff(
    pause_base = pause_base,
    max_times = max_times,
    pause_cap = pause_cap
  )

  insistently_read <- insistently(
    s3$select_object_content,
    rate = rate,
    quiet = FALSE
  )

  result <- insistently_read(
    Bucket = s3_bucket_key_ext$bucket,
    Key = s3_bucket_key_ext$key,
    Expression = Expression,
    ExpressionType = ExpressionType,
    InputSerialization = InputSerialization,
    OutputSerialization = OutputSerialization,
    ...
  )

  read.csv(
    text = result$Payload$Records$Payload,
    header = InputSerialization$CSV$FileHeaderInfo == "NONE" %||% FALSE
  )
}
