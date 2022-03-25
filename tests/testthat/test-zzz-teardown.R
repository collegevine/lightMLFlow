test_that("Teardown", {
  rm(
    list = ls(lightMLFlow:::.globals),
    envir = lightMLFlow:::.globals
  )

  purrr::walk(
    list_experiments()$experiment_id,
    delete_experiment
  )

  expect_null(list_experiments())
})
