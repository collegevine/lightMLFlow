#!/bin/bash
set -eo pipefail

apt-get update && apt-get install docker.io -y

heroku container:login

heroku config:set \
  S3_URI="$S3_URI" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION" \
  MLFLOW_TRACKING_USERNAME="$MLFLOW_TRACKING_USERNAME" \
  MLFLOW_TRACKING_PASSWORD="$MLFLOW_TRACKING_PASSWORD" \
  --app lightmlflow-testing

heroku container:push web --app lightmlflow-testing
heroku container:release web --app lightmlflow-testing
