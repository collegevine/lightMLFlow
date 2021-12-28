test_that("Experiment creation / renaming / deletion / reactivation work", {

  n1 <- paste0(
    "unit-test-",
    get_timestamp()
  )

  n2 <- paste0(
    "unit-test-",
    get_timestamp()
  )

  id1 <- create_experiment(
    name = n1
  )

  id2 <- create_experiment(
    name = n2,
    tags = c(
      a = "b",
      c = "d"
    )
  )

  by_id <- get_experiment(
    experiment_id = id1
  )

  by_name <- get_experiment(
    name = n1
  )

  expect_identical(
    by_id,
    by_name
  )

  expect_equal(
    by_id$experiment_id,
    id1
  )

  expect_equal(
    get_experiment(id2)$tags.key,
    c("a", "c")
  )

  all_experiments <- list_experiments()

  all_experiments %>%
    subset(
      name == n1
    ) %>%
    nrow() %>%
    expect_equal(1)

  all_experiments %>%
    subset(
      name == n2
    ) %>%
    nrow() %>%
    expect_equal(2)

  delete_experiment(id1)
  delete_experiment(id2)

  list_experiments() %>%
    subset(
      name %in% c(n1, n2)
    ) %>%
    nrow() %>%
    expect_equal(0)

  expect_equal(
    get_experiment(id1)$lifecycle_stage[1],
    "deleted"
  )

  restore_experiment(id1)

  expect_equal(
    get_experiment(id1)$lifecycle_stage[1],
    "active"
  )

  delete_experiment(id1)

  expect_equal(
    get_experiment(id1)$lifecycle_stage[1],
    "deleted"
  )

  restore_experiment(id1)

  new_name <- paste0("test-name", runif(1, 0, 1000000))
  rename_experiment(
    new_name,
    id1
  )

  expect_equal(
    get_experiment(id1)$name[1],
    new_name
  )
})
