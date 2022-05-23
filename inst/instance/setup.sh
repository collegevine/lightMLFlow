#!/bin/bash
set -eo pipefail

heroku login
heroku container:login

heroku create lightmlflow-testing
heroku addons:create heroku-postgresql:hobby-dev --app lightmlflow-testing


