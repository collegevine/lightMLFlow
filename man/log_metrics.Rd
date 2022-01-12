% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/runs.R
\name{log_metrics}
\alias{log_metrics}
\title{Log Metrics}
\usage{
log_metrics(..., timestamp, step, run_id, client)
}
\arguments{
\item{...}{variable names from which a data.frame with \code{key} and \code{value} columns will be created.}

\item{timestamp}{Timestamp at which to log the metric. Timestamp is rounded to the nearest
integer. If unspecified, the number of milliseconds since the Unix epoch is used.}

\item{step}{Step at which to log the metric. Step is rounded to the nearest integer. If
unspecified, the default value of zero is used.}

\item{run_id}{A run uuid. Automatically inferred if a run is currently active.}

\item{client}{An MLFlow client. Defaults to \code{NULL} and will be auto-generated.}
}
\description{
Logs a metric for a run. Metrics key-value pair that records a single float measure.
During a single execution of a run, a particular metric can be logged several times.
The MLflow Backend keeps track of historical metric values along two axes: timestamp and step.
}