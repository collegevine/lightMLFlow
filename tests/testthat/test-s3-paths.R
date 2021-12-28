test_that("S3 path creation works as intended", {

  prefix <- "mlflow-examples"
  experiment_id <- "1"
  run_id = "foo"
  fname = "bar.baz"

  uri <- create_s3_path(
    prefix,
    experiment_id,
    run_id,
    fname
  )

  expect_equal(
    uri,
    paste(
      prefix,
      experiment_id,
      run_id,
      "artifacts",
      fname,
      sep = "/"
    )
  )
})

test_that("Getting S3 bucket works", {
  expect_error(
    get_s3_bucket_and_prefix("foobar"),
    "Your S3_URI"
  )

  s3_info <- get_s3_bucket_and_prefix("s3://foo/bar/baz-qux")

  expect_equal(
    s3_info$bucket,
    "foo"
  )

  expect_equal(
    s3_info$prefix,
    "bar/baz-qux"
  )
})
