new_mlflow_client <- function(tracking_uri) {
  UseMethod("new_mlflow_client")
}

#' @importFrom rlang abort
new_mlflow_uri <- function(raw_uri) {
  if (!grepl("://", raw_uri)) abort("The `tracking_uri` you provided didn't contain '://'. Please provide a valid URI and try again.")
  parts <- strsplit(raw_uri, "://")[[1]]

  structure(
    list(scheme = parts[1], path = parts[2]),
    class = c(paste("mlflow_", parts[1], sep = ""), "mlflow_uri")
  )
}

new_mlflow_client_impl <- function(get_host_creds, get_cli_env = list, class = character()) {
  structure(
    list(
      get_host_creds = get_host_creds,
      get_cli_env = get_cli_env
    ),
    class = c(class, "mlflow_client")
  )
}

new_mlflow_host_creds <- function(host = NA, username = NA, password = NA, token = NA,
                                  insecure = "False") {

  insecure_arg <- if (is.null(insecure) || is.na(insecure) || insecure == "") {
    "False"
  } else {
    list(true = "True", false = "False")[[tolower(insecure)]]
  }
  structure(
    list(
      host = host, username = username, password = password, token = token,
      insecure = insecure_arg
    ),
    class = "mlflow_host_creds"
  )
}

create_creds <- function(mlflow_host_creds, part) {
  if (is.na(mlflow_host_creds[[part]])) {
    ""
  } else {
    paste(part, mlflow_host_creds[[part]], sep = " = ")
  }
}

#' @export
print.mlflow_host_creds <- function(x, ...) {
  args <- list(
    host = create_creds(x, "host"),
    username = create_creds(x, "username"),
    password = create_creds(x, "password"),
    token = create_creds(x, "token"),
    insecure = create_creds(x, "insecure")
  )

  cat("mlflow_host_creds( ")
  do.call(cat, args[args != ""])
  cat(")\n")
}

new_mlflow_client.default <- function(tracking_uri) {
  abort(paste("Unsupported scheme: '", tracking_uri$scheme, "'", sep = ""))
}

basic_http_client <- function(tracking_uri) {
  host <- paste(tracking_uri$scheme, tracking_uri$path, sep = "://")

  get_host_creds <- function() {
    new_mlflow_host_creds(
      host = host,
      username = Sys.getenv("MLFLOW_TRACKING_USERNAME"),
      password = Sys.getenv("MLFLOW_TRACKING_PASSWORD"),
      token = Sys.getenv("MLFLOW_TRACKING_TOKEN"),
      insecure = Sys.getenv("MLFLOW_TRACKING_INSECURE")
    )
  }

  cli_env <- function() {
    creds <- get_host_creds()
    res <- list(
      MLFLOW_TRACKING_USERNAME = creds$username,
      MLFLOW_TRACKING_PASSWORD = creds$password,
      MLFLOW_TRACKING_TOKEN = creds$token,
      MLFLOW_TRACKING_INSECURE = creds$insecure
    )
    res[!is.na(res)]
  }

  new_mlflow_client_impl(get_host_creds, cli_env, class = "mlflow_http_client")
}

new_mlflow_client.mlflow_http <- function(tracking_uri) {
  basic_http_client(tracking_uri)
}

new_mlflow_client.mlflow_https <- function(tracking_uri) {
  basic_http_client(tracking_uri)
}

#' Initialize an MLflow Client
#'
#' Initializes and returns an MLflow client that communicates with the tracking server or store
#' at the specified URI.
#'
#' @param tracking_uri The tracking URI. If not provided, defaults to the service
#'  set by `mlflow_set_tracking_uri()`.
#'
#' @importFrom rlang `%||%`
#' @export
mlflow_client <- function(tracking_uri = NULL) {

  tracking_uri <- new_mlflow_uri(tracking_uri %||% get_tracking_uri())
  client <- new_mlflow_client(tracking_uri)

  if (inherits(client, "mlflow_file_client")) validate_server(client)

  client
}
