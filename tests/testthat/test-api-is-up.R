test_that("API is up", {

  skip_on_cran()
  .alive <- function() {
    tryCatch(
      check_api_status(),
      error = function(e) "not ok"
    )
  }

  .difftime <- function(starttime, currtime = Sys.time()) {
    as.numeric(currtime - starttime)
  }

  status <- ""
  starttime <- Sys.time()
  tries <- 0

  while(status != "ok" & (.difftime(starttime) < 180)) {

    if (tries > 0) Sys.sleep(10)
    status <- .alive()

    tries <- tries + 1
  }

  expect_equal(
    status,
    "ok"
  )
})
