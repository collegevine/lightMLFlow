#' Save Model for MLflow
#'
#' Saves model in MLflow format that can later be used for prediction and serving. This method is
#' generic to allow package authors to save custom model types.
#'
#' @param model The model that will perform a prediction.
#' @param path Destination path where this MLflow compatible model
#'   will be saved.
#' @param model_spec MLflow model config this model flavor is being added to.
#' @param ... Additional arguments to pass to `log_artifact`
#' @importFrom yaml write_yaml
#' @export
save_model <- function(model, path, model_spec = list(), ...) {
  UseMethod("save_model")
}

#' Log Model
#'
#' Logs a model for this run. Similar to `save_model()`
#' but stores model as an artifact within the active run.
#'
#' @param model The model that will perform a prediction.
#' @param path Destination path where this MLflow compatible model
#'   will be saved.
#' @param ... Optional additional arguments passed to `save_model()` when persisting the
#'   model. For example, `conda_env = /path/to/conda.yaml` may be passed to specify a conda
#'   dependencies file for flavors (e.g. keras) that support conda environments.
#'
#' @importFrom fs path_temp
#' @export
log_model <- function(model, path, ...) {

  model_spec <- save_model(
    model,
    path = path,
    model_spec = list(
      utc_time_created = get_timestamp(),
      run_id = get_active_run_id_or_start_run(),
      artifact_path = path,
      flavors = list()
    )
  )

  model_fname <- model_spec$object_name

  log_artifact(
    paste(path, model_fname, sep = "/"),
    ...
  )

  log_artifact(
    paste(path, "MLmodel", sep = "/"),
    ...
  )

  unlink(file.path(path, model_fname))
  unlink(file.path(path, "MLmodel"))

  res <- tryCatch(
    {
      record_logged_model(model_spec)
    },
    error = function(e) {
      warning(paste("Logging model metadata to the tracking server has failed, possibly due to older",
        "server version. The model artifacts have been logged successfully.",
        "In addition to exporting model artifacts, MLflow clients 1.7.0 and above",
        "attempt to record model metadata to the  tracking store. If logging to a",
        "mlflow server via REST, consider  upgrading the server version to MLflow",
        "1.7.0 or above.",
        sep = " "
      ))
    }
  )

  res
}

#' @importFrom purrr compact
write_model_spec <- function(path, content) {
  write_yaml(
    compact(content),
    file.path(path, "MLmodel")
  )
}

#' @importFrom withr with_options
get_timestamp <- function() {
  with_options(
    c(digits.secs = 2),
    format(
      as.POSIXlt(Sys.time(), tz = "GMT"),
      "%y-%m-%dT%H:%M:%S.%OS"
    )
  )
}

#' Load MLflow Model
#'
#' Loads an MLflow model. MLflow models can have multiple model flavors. Not all flavors / models
#' can be loaded in R. This method by default searches for a flavor supported by R/MLflow.
#'
#' @param model_uri The URI to the model to load.
#' @param flavor Optional flavor specification (string). Can be used to load a particular flavor in
#' case there are multiple flavors available.
#' @param client An MLFlow client. Defaults to `NULL` and will be auto-generated.
#'
#' @importFrom yaml read_yaml
#' @importFrom fs path
#' @export
load_model <- function(model_uri, flavor = NULL, client = mlflow_client()) {
  ## Do nothing for now

  invisible()
}

new_mlflow_flavor <- function(class = character(0), spec = NULL) {
  flavor <- structure(character(0), class = c(class, "mlflow_flavor"))
  attributes(flavor)$spec <- spec

  flavor
}

# Create an MLflow Flavor Object
#
# This function creates an `mlflow_flavor` object that can be used to dispatch
#   the `load_flavor()` method.
#
# @param flavor The name of the flavor.
# @keywords internal
mlflow_flavor <- function(flavor, spec) {
  new_mlflow_flavor(
    class = paste0(
      "mlflow_flavor_",
      flavor
    ),
    spec = spec
  )
}

#' Load MLflow Model Flavor
#'
#' Loads an MLflow model using a specific flavor. This method is called internally by
#' `load_model`, but is exposed for package authors to extend the supported
#' MLflow models. See https://mlflow.org/docs/latest/models.html#storage-format for more
#' info on MLflow model flavors.
#'
#' @param flavor An MLflow flavor object loaded by `load_model`, with class
#' loaded from the flavor field in an MLmodel file.
#' @param model_path The path to the MLflow model wrapped in the correct
#'   class.
#'
#' @export
load_flavor <- function(flavor, model_path) {
  UseMethod("load_flavor")
}

#' @importFrom utils methods
supported_model_flavors <- function() {
  map(
    methods(generic.function = load_flavor),
    ~ gsub("load_flavor\\.mlflow_flavor_", "", .x)
  )
}

# Helper function to parse data frame from json based on given the json_fomat.
# The default behavior is to parse the data in the Pandas "split" orient.
parse_json <- function(input_path, json_format = "split") {
  switch(json_format,
    split = {
      json <- fromJSON(input_path, simplifyVector = TRUE)
      elms <- names(json)
      if (length(setdiff(elms, c("columns", "index", "data"))) != 0 ||
        length(setdiff(c("columns", "data"), elms) != 0)) {
        stop(paste("Invalid input. Make sure the input json data is in 'split' format.", elms))
      }
      df <- data.frame(json$data, row.names = json$index)
      names(df) <- json$columns
      df
    },
    records = fromJSON(input_path, simplifyVector = TRUE),
    stop(paste(
      "Unsupported JSON format", json_format,
      ". Supported formats are 'split' or 'records'"
    ))
  )
}
