
<!-- README.md is generated from README.Rmd. Please edit that file -->

# lightMLFlow

A lightweight R wrapper for the MLFlow REST API

<!-- badges: start -->
[![R-CMD-check](https://github.com/collegevine/lightMLFlow/workflows/R-CMD-check/badge.svg)](https://github.com/collegevine/lightMLFlow/actions)
<!-- badges: end -->

# Setup

`lightMLFlow` is not on CRAN. You can install it from Github as follows:

``` r
# install.packages("devtools")

devtools::install_github("collegevine/lightMLFlow")
```

# The Package

This package differs from the CRAN `mlflow` package in a few important
ways.

First, there are some things that the full CRAN `mlflow` package has
that this package doesn’t:

-   `mlflow` supports more configurable artifact stores, while
    `lightMLFlow` only supports S3 as the artifact store.
-   `mlflow` allows you to run `install_mlflow()` to install MLFlow on
    your machine. `lightMLFlow` assumes you already have `MLFlow`
    installed. (In other words, there’s no `install_mlflow()` function
    in this package).
-   `mlflow` lets you run the MLFlow UI directly from the package, which
    `lightMLFlow` does not. `lightMLFlow` is made to be used with a
    deployed MLFlow instance, which means that your instance needs to
    already exist (either on local on on a cloud server).

However, there are also significant advantages to using `lightMLFlow`
over `mlflow`:

-   `lightMLFlow` features a friendlier API, with significantly fewer
    functions, no `mlflow::mlflow_*` function prefixing (following
    Tidyverse conventions, `lightMLFlow` function names are verbs), and
    improved error handling.
-   `lightMLFlow` fixes some bugs in `mlflow`’s API wrapping functions.
-   `lightMLFlow` is significantly more lightweight than `mlflow`. It
    doesn’t depend on `httpuv`, `reticulate`, or `swagger`, and has a
    more minimal footprint in general.
-   `lightMLFlow` uses `aws.s3` to put and save objects to and from S3,
    which means you don’t need to have a `boto3` install on your machine
    running your `MLFlow` code. This is an essential change, as it means
    that `lightMLFlow` does not require *any* Python infrastructure, as
    opposed to `mlflow`, which does.

## Future Work

A couple goals for this package as it matures:

1.  Supporting `Tidymodels` model formats in `save_model` and
    `log_model`. Since `Tidymodels` provides a uniform API for modeling,
    it should play nicely with `MLFlow` by allowing us to save an
    arbitrary model in `.rds` format (as long as a `predict` method
    exists), and then load the `.rds` file and use
    `predict(model, data)` when serving the model.
