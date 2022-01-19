if (interactive()) {
  Sys.setenv(
    MLFLOW_TRACKING_URI = get_secret("MLFLOW_TRACKING_URI"),
    AWS_ACCESS_KEY_ID = get_secret("AWS_ACCESS_KEY_ID"),
    AWS_DEFAULT_REGION = get_secret("AWS_DEFAULT_REGION"),
    AWS_SECRET_ACCESS_KEY = get_secret("AWS_SECRET_ACCESS_KEY"),
    MLFLOW_TRACKING_PASSWORD = get_secret("MLFLOW_TRACKING_PASSWORD"),
    MLFLOW_TRACKING_USERNAME = get_secret("MLFLOW_TRACKING_USERNAME"),
    S3_URI = get_secret("S3_URI")
  )
}
