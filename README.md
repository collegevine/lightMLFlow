
<!-- README.md is generated from README.Rmd. Please edit that file -->

# lightMLFlow

A lightweight R wrapper for the MLFlow REST API

<!-- badges: start -->
[![R-CMD-check](https://github.com/collegevine/lightMLFlow/workflows/R-CMD-check/badge.svg)](https://github.com/collegevine/lightMLFlow/actions)
[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
<!-- badges: end -->

# Setup

`lightMLFlow` will soon be on CRAN!

For now, you can install it from Github as follows:

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
    `lightMLFlow` only supports S3 as the artifact store. That said, we
    would love contributions from Azure or GCP users to extend this
    functionality!
-   `mlflow` allows you to run `install_mlflow()` to install MLFlow on
    your machine. `lightMLFlow` assumes you’re using an MLFlow instance
    that’s either running locally or hosted on a cloud server.
-   `mlflow` lets you run the MLFlow UI directly from the package, which
    `lightMLFlow` does not. Again, `lightMLFlow` is made to be used with
    a deployed MLFlow instance, which means that your instance needs to
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
-   `mlflow` (and, specifically, `MLFlow Projects`) doesn’t play
    particularly nicely with `renv`. The reason for that is that an
    `MLProject` file that’s pointed at a Git repo will try to clone and
    run the code from scratch. But with `renv`, we like restoring a
    package cache in CI and baking it into the Docker image that the
    code lives in so that we don’t need to install all of the R packages
    the project needs every time we run the project. `lightMLFlow` hacks
    its way around this problem by allowing the user to run
    `set_git_tracking_tags()`, which tricks the MLFlow REST API into
    thinking that the code was run from an MLFlow Project even when it
    wasn’t. This lets you keep your normal (e.g.) `renv` workflow in
    place and get the benefit of linked Git commits in the MLFlow UI
    without actually needing any of the `MLProject` infrastructure or
    setup steps.
-   For artifact and model logging, `lightMLFlow` logs R objects
    directly so that you don’t need to worry about first saving a file
    to disk and then copying it to your artifact store.
-   In addition, `lightMLFlow` allows artifacts to be loaded directly
    into the R session in one shot, instead of first being saved to disk
    and then loaded afterwards. This eliminates lines of code and the
    headache associated with going S3 –> disk –> R by abstracting away
    the disk reads and writes.
-   In `lightMLFlow`, `create_experiment` returns the experiment when
    one with the specified name already exists, instead of erroring.
-   `lightMLFlow` adds `get_param` and `get_metric` helpers to make it
    easier to get the most recent value of a metric or param for a run.
-   `lightMLFlow` leverages some R-ish ways of doing things in reworking
    `log_params` and `log_metrics`, which both take dot args of metrics
    or params to log, and then call `log_batch()` on the backend. This
    results in a far friendlier API than needing to specify a key and
    value separately and forcing the user to make multiple calls to
    `log_param` (e.g.) for each key-value pair. With `lightMLFlow`, you
    can do this to log two params: `log_params(foo, bar = "baz")`, which
    will log the value of `foo` as `foo` (it automagically generates the
    param name by deparsing the name of the R object), and will log
    `bar` as `"baz"`.
-   When logging metrics, `lightMLFlow` abstracts away timestamps and
    steps from the user, automatically setting the timestamp to the
    current time (UTC) and auto-incrementing the step if the metric
    being logged already exists.
-   If a run errors out, `lightMLFlow` logs the error as an artifact (a
    markdown document) to help with the debugging process.

## Known Issues / Future Work

1.  Clarify the parameter names for things like `path`, `model_path`,
    etc. since they don’t make much sense right now.
2.  Clarify the difference between `save_model` and `log_model`.
3.  Add Azure and GCP artifact stores.
