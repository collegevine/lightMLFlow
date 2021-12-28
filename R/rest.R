mlflow_rest_path <- function(version) {
  switch(version,
    "2.0" = "api/2.0/mlflow"
  )
}

#' @importFrom httr timeout
mlflow_rest_timeout <- function() {
  timeout(getOption("mlflow.rest.timeout", 60))
}

try_parse_response_as_text <- function(response) {
  raw_content <- content(response, type = "raw")
  tryCatch(
    {
      rawToChar(raw_content)
    },
    error = function(e) {
      do.call(paste, as.list(raw_content))
    }
  )
}

#' @importFrom base64enc base64encode
#' @importFrom utils packageVersion
#' @importFrom httr config
get_rest_config <- function(host_creds) {

  headers <- list()

  auth_header <- if (!is.na(host_creds$username) && !is.na(host_creds$password)) {
    basic_auth_str <- paste(host_creds$username, host_creds$password, sep = ":")
    paste("Basic", base64encode(charToRaw(basic_auth_str)), sep = " ")
  } else if (!is.na(host_creds$token)) {
    paste("Bearer", host_creds$token, sep = " ")
  } else {
    NA
  }

  if (!is.na(auth_header)) {
    headers$Authorization <- auth_header
  }

  headers$`User-Agent` <- paste("mlflow-r-client", packageVersion("lightMLFlow"), sep = "/")
  is_insecure <- as.logical(host_creds$insecure)

  list(
    headers = headers,
    config = if (is_insecure) {
      config(ssl_verifypeer = 0, ssl_verifyhost = 0)
    } else {
      list()
    }
  )
}

#' @importFrom httr GET POST PATCH DELETE add_headers config content
#' @importFrom jsonlite fromJSON
#' @importFrom rlang warn
mlflow_rest <- function(..., client, query = NULL, data = NULL, verb = "GET", version = "2.0",
                        max_rate_limit_interval = 60) {

  host_creds <- client$get_host_creds()
  rest_config <- get_rest_config(host_creds)
  args <- list(...)

  api_url <- file.path(
    host_creds$host,
    mlflow_rest_path(version),
    paste(args, collapse = "/")
  )

  req_headers <- do.call(add_headers, rest_config$headers)

  get_response <- switch(verb,
    GET = function() {
      GET(
        api_url,
        query = query,
        mlflow_rest_timeout(),
        config = rest_config$config,
        req_headers
      )
    },
    POST = function() {
      POST(
        api_url,
        body = toJSON(
          data,
          pretty = TRUE,
          auto_unbox = TRUE
        ),
        mlflow_rest_timeout(),
        config = rest_config$config,
        req_headers
      )
    },
    PATCH = function() {
      PATCH(
        api_url,
        body = toJSON(
          data,
          pretty = TRUE,
          auto_unbox = TRUE
        ),
        mlflow_rest_timeout(),
        config = rest_config$config,
        req_headers
      )
    },
    DELETE = function() {
      DELETE(
        api_url,
         body = toJSON(
           data,
           pretty = TRUE,
           auto_unbox = TRUE
         ),
        mlflow_rest_timeout(),
        config = rest_config$config,
        req_headers
      )
    },
    stop("Verb '", verb, "' is unsupported.", call. = FALSE)
  )

  sleep_for <- 1
  time_left <- max_rate_limit_interval
  response <- get_response()

  while (response$status_code == 429 && time_left > 0) {
    time_left <- time_left - sleep_for
    warn(
      paste(
        "Request returned with status code 429 (Rate limit exceeded). Retrying after",
        sleep_for,
        "seconds. Will continue to retry 429s for up to",
        time_left,
        "second."
      )
    )

    Sys.sleep(sleep_for)
    sleep_for <- min(time_left, sleep_for * 2)
    response <- get_response()
  }

  stop_for_status(response)

  content(
    x = response,
    as = "parsed",
    encoding = "UTF-8"
  )
}

#' Internal function to warn if API call was unsuccessful
#'
#' The function is called for the side effect of warning when the API response
#' has errors, and is a thin wrapper around httr::stop_for_status
#'
#' @param r The response from a call to the MLFlow API
#'
#' @return NULL
#' @importFrom httr status_code content
#' @keywords Internal
#' @noRd
#'
stop_for_status <- function(r) {
  # note that httr::stop_for_status should be called explicitly

  if (r$status_code != 200) {
    cr <- content(
      x = r,
      as = "parsed",
      encoding = "UTF-8"
    )

    stop(
      sprintf(
        "MLFlow API call failed with status code %s.\n\nDiagnostics:\n%s",
        r$status_code,
        paste(
          cr,
          collapse = "\n"
        )
      ),
      call. = FALSE
    )
  }

  invisible()
}

