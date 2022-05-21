FROM continuumio/miniconda3

## Copy files into the image
COPY run.sh run.sh
COPY requirements.txt requirements.txt
COPY nginx.conf_template /etc/nginx/sites-available/default/nginx.conf_template

## Install Postgres
RUN apt-get -y update && \
  apt-get -y upgrade && \
  apt-get install -y postgresql

## Install nginx and dependencies
RUN apt-get -y update && \
  apt-get install -y make vim \
  automake gcc g++ subversion \
  musl-dev nginx gettext apache2-utils

## Install pip and dependencies
RUN conda install -c anaconda pip && \
  pip install --upgrade pip && \
  pip install -r requirements.txt && \
  conda update -n base -c defaults conda && \
  conda env list && \
  pip freeze list

## Run your `run.sh` script on container boot
CMD ./run.sh
