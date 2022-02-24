test_that("Utils work", {
  a <- 1
  b <- "c"
  kv <- get_key_value_df(
    a,
    b,
    c = TRUE,
    .which = 0
  )
  expect_equal(
    kv,
    tibble(
      key = c("a", "b", "c"),
      value = c(1, "c", TRUE)
    )
  )

  expect_equal(
    param_value_to_rest(
      c(3.14, NaN, -Inf, Inf, 0)
    ),
    as.character(c(3.14, "NaN", "-Infinity", "Infinity", 0))
  )
  metrics <- tibble(
    key = c("a", "b"),
    value = c(1, 2)
  )
  timestamp <- c(3, 4, 5)
})
