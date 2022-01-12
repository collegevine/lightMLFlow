test_that("Runs work", {
  run_test_experiment <- create_experiment(
    paste0(
      "integration-test-",
      get_timestamp()
    )
  )

  start_run(experiment_id = run_test_experiment)

  model <- lm(pressure ~ temperature, data = pressure)

  log_artifact(
    x = model,
    FUN = saveRDS,
    filename = "model.rds"
  )

  model_summary <- summary(model)

  r2 <- model_summary$r.squared
  f <- model_summary$fstatistic[["value"]]

  log_metrics(
    "R2" = R2,
    "F" = f
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

  expect_equal(
    r2_hist$value,
    c(r2, 1)
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
