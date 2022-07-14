test_that("Runs work", {
  skip_on_cran()
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

  expect_error(
    create_model_version(
      experiment_name,
      source = NULL
    )
  )

  create_model_version(
    experiment_name
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

  logging_fun <- function(x, ...) {
    stop("Failing intentionally")
  }

  expect_error(
    suppressMessages({
      log_artifact(
        x = model,
        FUN = logging_fun,
        filename = "foobarbaz"
      )
    }),
    "Request failed after 5 attempts"
  )

  expect_error(
    suppressMessages({
      capture.output({
        load_artifact(
          artifact_name = "foobarbaz",
          FUN = logging_fun
        )
      })
    }),
    "Request failed after 5 attempts"
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
    as.Date(as.POSIXct(r2_history$timestamp, origin="1970-01-01", tz = "UTC")),
    as.Date(as.POSIXct(Sys.time(), origin="1970-01-01", tz = "UTC"))
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

  Sys.sleep(3)

  p_batch <- data.frame(
    key = c("intercept", "temperature"),
    value = unname(coef(model))
  )

  log_batch(
    params = p_batch
  )

  i <- get_param("intercept")

  expect_equal(
    i,
    as.character(unname(coef(model))[1])
  )

  m_batch <- data.frame(
    key = c("R2", "F"),
    value = c(1, 100),
    step = c(2,1),
    timestamp = get_timestamp() %>% convert_timestamp_to_ms()
  )

  log_batch(
    metrics = m_batch
  )

  curr_r2 <- get_metric("R2")

  expect_equal(
    curr_r2,
    1
  )

  r2_history <- get_metric_history("R2")

  expect_identical(
    r2_history$step,
    0L:2L
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
    c(round(R2, 4), 1, 1)
  )

  expect_gt(
    r2_hist$timestamp[3],
    r2_hist$timestamp[2]
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

  ## Tests for SELECT API
  log_artifact(
    iris,
    write.csv,
    "iris.csv"
  )

  result <- s3_select_from_artifact(
    "iris.csv",
    Expression = "SELECT \"Sepal.Length\" AS sl FROM s3object s WHERE CAST(\"Sepal.Length\" AS FLOAT) >= 5",
    InputSerialization = list(CSV = list(FileHeaderInfo = "USE", RecordDelimiter = "\n", FieldDelimiter = ","), CompressionType = "NONE")
  )

  expect_equal(
    nrow(result),
    nrow(subset(iris, iris$Sepal.Length >= 5))
  )

  expect_equal(
    ncol(result),
    1
  )

  end_run()
})

test_that("Metric logging works outside of a run", {
  skip_on_cran()
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

  foo_history <- get_metric_history(metric_key = "foo", run_id = run_id)

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

  foo_history <- get_metric_history(metric_key = "foo", run_id = run_id)

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

test_that("Searching runs works", {
  skip_on_cran()
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

  log_params(
    bar = 456
  )

  end_run()

  start_run(
    experiment_id = metric_test_experiment
  )

  ## this should work even if metrics is NULL and end_time is NA
  expect_equal(
    nrow(search_runs(metric_test_experiment)),
    2
  )

  end_run()

  expect_equal(
    nrow(search_runs(metric_test_experiment, run_view_type = "DELETED")),
    0
  )
})
