test_that("Model registry works", {
  experiment_name <- paste0(
    "integration-test-",
    get_timestamp()
  )

  run_test_experiment <- create_experiment(
    experiment_name
  )

  start_run(experiment_id = run_test_experiment)

  model <- create_registered_model(
    experiment_name
  )

  all_models <- list_registered_models(parse = FALSE)

  all_model_names <- all_models$registered_models %>%
    purrr::transpose() %>%
    purrr::pluck("name") %>%
    unlist()

  expect_true(
    model$name %in% all_model_names
  )

  all_models <- list_registered_models(parse = TRUE)

  expect_true(is.data.frame(all_models$registered_models))

  expect_equal(
    names(get_registered_model(model$name)),
    c("name", "creation_timestamp", "last_updated_timestamp")
  )

  new_name <- paste0(experiment_name, "2")
  rename_registered_model(
    experiment_name,
    new_name
  )

  all_model_names <- list_registered_models() %>%
    purrr::pluck("registered_models") %>%
    purrr::transpose() %>%
    purrr::pluck("name") %>%
    unlist()

  expect_true(
    new_name %in% all_model_names
  )

  rename_registered_model(
    new_name,
    model$name
  )

  model_version <- create_model_version(
    model$name,
    source = ""
  )

  get_model_version(
    model_version$name,
    model_version$version
  ) %>%
    purrr::pluck("last_updated_timestamp") %>%
    expect_equal(model_version$last_updated_timestamp)

  version <- get_latest_versions(
    model$name,
    stages = list("None")
  )

  end_run()
})
