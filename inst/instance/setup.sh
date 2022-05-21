#!/bin/bash
set -eo pipefail

heroku login

heroku create lightmlflow-testing
heroku addons:create heroku-postgresql:hobby-dev --app lightmlflow-testing


