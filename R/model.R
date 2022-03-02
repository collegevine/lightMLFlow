#' @importFrom withr with_options
get_timestamp <- function() {
  format(
    as.POSIXlt(Sys.time(), tz = "UTC"),
    "%y-%m-%dT%H:%M:%S.%OS"
  )
}

convert_timestamp_to_ms <- function(timestamp) {
  ts <- as.POSIXct(
    timestamp,
    format = "%y-%m-%dT%H:%M:%S.%OS",
    tz = "UTC"
  )

  as.integer(ts) * 1000
}
