if (interactive()) {
  Sys.setenv(
    MLFLOW_TRACKING_URI = secret::get_secret("MLFLOW_TRACKING_URI"),
    AWS_ACCESS_KEY_ID = secret::get_secret("AWS_ACCESS_KEY_ID"),
    AWS_DEFAULT_REGION = secret::get_secret("AWS_DEFAULT_REGION"),
    AWS_SECRET_ACCESS_KEY = secret::get_secret("AWS_SECRET_ACCESS_KEY"),
    MLFLOW_TRACKING_PASSWORD = secret::get_secret("MLFLOW_TRACKING_PASSWORD"),
    MLFLOW_TRACKING_USERNAME = secret::get_secret("MLFLOW_TRACKING_USERNAME"),
    S3_URI = secret::get_secret("S3_URI")
  )
}
