# lightMLFlow 0.6.7

* Fixes a bug with `search_runs` where `rbind`-ing runs with different numbers of columns (e.g. possible due to no logged parameters or no end time for a run) resulted in an error.

# lightMLFlow 0.6.6

* Uses `purrr::insistently` to retry artifact saves and loads, since S3's API has a habit of returning a `500` every once in a while. Five retries by default, with an exponential backoff.

# lightMLFlow 0.6.5

* Fixes a bug where MLFlow `1.26.0+` now requires the `source` parameter when creating a model version. In `lightMLFlow`, this is defaulted to the artifact URI of the current run.
* CI improvements

# lightMLFlow 0.6.0

* Rips out lots of unused code that's only useful for MLFlow running on local (which is not the goal of this project to enable)
* Rips out the MLFlow `models` infrastructure. The idea for this package -- at least in the short-term -- is to save models as artifacts with the intention of using them in Plumber APIs, etc. If you wanted to load an (e.g.) XGBoost model into Sagemaker, you could use `log_artifact` in tandem with `FUN = xgboost::xgb.save` to make this easy.
* Adds some more helpful hints for MLFlow clients

# lightMLFlow 0.5.1

* Fixes a bug that would leave a GH token in plain text in the run

# lightMLFlow 0.5.0

* Added a `NEWS.md` file to track changes to the package.
* Adds `get_metric`
* Adds `get_param`
* Abstracts away more timestamp headaches from the user
