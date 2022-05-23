export HEROKU_PORT=$(echo "$PORT")
export MLFLOW_PORT=5000

envsubst '$HEROKU_PORT,$MLFLOW_PORT' < /etc/nginx/sites-available/default/nginx.conf_template > /etc/nginx/sites-available/default/nginx.conf

htpasswd -bc /etc/nginx/.htpasswd $MLFLOW_TRACKING_USERNAME $MLFLOW_TRACKING_PASSWORD

killall nginx

mlflow ui \
  --port $MLFLOW_PORT \
  --host 127.0.0.1 \
  --backend-store-uri $(echo "$DATABASE_URL" | sed "s/postgres/postgresql/") \
  --default-artifact-root $S3_URI &

nginx -g 'daemon off;' -c /etc/nginx/sites-available/default/nginx.conf
