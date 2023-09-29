test_that("Teardown", {
  rm(
    list = ls(lightMLFlow:::.globals),
    envir = lightMLFlow:::.globals
  )

  purrr::walk(
    search_experiments()$experiment_id,
    delete_experiment
  )

  expect_null(search_experiments())
})
