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
