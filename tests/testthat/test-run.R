test_that("Runs work", {
  experiment_name <- paste0(
    "integration-test-",
    get_timestamp()
  )

  run_test_experiment <- create_experiment(
    experiment_name
  )

  start_run(experiment_id = run_test_experiment)

  model <- lm(pressure ~ temperature, data = pressure)

  ## Need both of these for a model that get_registered_model_run_id will detect.
  create_registered_model(
    experiment_name
  )

  create_model_version(
    experiment_name,
    source = Sys.getenv("S3_URI")
  )

  log_artifact(
    x = model,
    FUN = saveRDS,
    filename = "model.rds"
  )

  expect_equal(
    list_artifacts()$path,
    "model.rds"
  )

  p <- ggplot2::ggplot(
    pressure,
    ggplot2::aes(x = temperature, y = pressure)
  ) +
    ggplot2::geom_point()

  log_artifact_path <- log_artifact(
    x = p,
    filename = "pressure.png",
    ## extra params passed to `...`
    device = "png",
    width = 6,
    height = 6,
    FUN = ggsave
  )

  model_loaded <- load_artifact(
    "model.rds"
  )

  expect_identical(
    model$coefficients,
    model_loaded$coefficients
  )

  expect_identical(
    model$residuals,
    model_loaded$residuals
  )

  expect_setequal(
    list_artifacts()$path,
    c("model.rds", "pressure.png")
  )
  expect_true(is.character(log_artifact_path))
  expect_equal(
    log_artifact_path,
    paste(get_artifact_path(), "pressure.png", sep = "/")
  )

  model_summary <- summary(model)

  R2 <- model_summary$r.squared
  f <- model_summary$fstatistic[["value"]]

  log_metrics(
    R2,
    "F" = f
  )

  r2_history <- get_metric_history("R2")

  expect_equal(
    lubridate::date(r2_history$timestamp),
    lubridate::today()
  )

  Sys.sleep(1)

  log_metrics(R2 = 1)

  r2_history <- get_metric_history("R2")

  expect_identical(
    r2_history$step,
    c(0L, 1L)
  )

  expect_gt(
    r2_history$timestamp[2],
    r2_history$timestamp[1]
  )

  expect_error(
    log_metrics(
      R2,
      "F" = f,
      "adj_r" = model_summary$adj.r.squared,
      timestamp = c(1, 2)
    )
  )

  Sys.sleep(3)

  log_model(
    model = carrier::crate(function(x) predict(model, x)),
    path = "carrier-model"
  )

  p_batch <- data.frame(
    key = c("intercept", "temperature"),
    value = unname(coef(model))
  )

  log_batch(
    params = p_batch
  )

  m_batch <- data.frame(
    key = c("R2", "F"),
    value = c(1, 100),
    step = 1,
    timestamp = round(as.numeric(as.POSIXlt(Sys.time(), tz = "UTC")))
  )

  log_batch(
    metrics = m_batch
  )

  log_params(
    "df1" = as.character(model_summary$df[1]),
    "df2" = as.character(model_summary$df[2])
  )

  r <- get_run()

  expect_equal(
    get_registered_model_run_id(experiment_name, stage = NULL),
    r$run_id
  )

  transition_model_version_stage(
    experiment_name,
    version = "1",
    stage = "Staging"
  )

  expect_equal(
    get_registered_model_run_id(experiment_name, stage = "Staging"),
    r$run_id
  )

  ## shouldn't be able to pass more than 1 stage into the function
  expect_error(
    get_registered_model_run_id(experiment_name, stage = c("Production", "Staging"))
  )

  expect_error(
    transition_model_version_stage(
      experiment_name,
      version = "1",
      stage = "bad"
    )
  )

  expect_equal(
    length(r$params[[1]]),
    4
  )

  expect_setequal(
    names(r$params[[1]]),
    c("intercept", "temperature", "df1", "df2")
  )

  expect_setequal(
    r$metrics[[1]]$key,
    c("R2", "F")
  )

  expect_equal(
    nrow(r$metrics[[1]]),
    2
  )

  expect_setequal(
    r$metrics[[1]]$value,
    c(1, 100)
  )

  r2_hist <- get_metric_history(
    "R2"
  )

  ## have to round because mlflow does some rounding when we get history?!?
  expect_equal(
    r2_hist$value,
    c(round(R2, 4), 1)
  )

  expect_gt(
    r2_hist$timestamp[2],
    r2_hist$timestamp[1]
  )

  expect_equal(
    unique(r2_hist$key),
    "R2"
  )

  expect_equal(
    get_active_experiment_id(),
    run_test_experiment
  )

  expect_equal(
    get_run()$run_id,
    get_active_run_id()
  )

  artifacts <- list_artifacts("carrier-model")

  artifacts %>%
    nrow() %>%
    expect_equal(2)

  expect_setequal(
    artifacts$path,
    c("carrier-model/MLmodel", "carrier-model/crate.bin")
  )

  end_run()
})

test_that("Metric logging works outside of a run", {
  experiment_name <- paste0(
    "metric-test-",
    get_timestamp()
  )

  metric_test_experiment <- create_experiment(
    experiment_name
  )

  start_run(
    experiment_id = metric_test_experiment
  )

  run_id <- get_active_run_id()

  log_metrics(
    foo = 123,
    step = 0
  )

  end_run()

  foo_history <- get_metric_history(metric = "foo", run_id = run_id)

  expect_equal(
    foo_history$step,
    0
  )
  expect_equal(
    foo_history$key,
    "foo"
  )
  expect_equal(
    foo_history$value,
    123
  )

  log_metrics(
    foo = 456,
    step = 1,
    run_id = run_id
  )

  foo_history <- get_metric_history("foo", run_id = run_id)

  expect_equal(
    nrow(foo_history),
    2
  )

  expect_identical(
    foo_history$value,
    c(123, 456)
  )

  expect_identical(
    foo_history$step,
    c(0L,1L)
  )
})
