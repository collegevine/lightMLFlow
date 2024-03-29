% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/experiments.R
\name{search_experiments_without_nodelete}
\alias{search_experiments_without_nodelete}
\alias{list_experiments_without_nodelete}
\title{List experiments without \code{NODELETE} tags}
\usage{
search_experiments_without_nodelete(
  view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"),
  client = mlflow_client()
)

list_experiments_without_nodelete(
  view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"),
  client = mlflow_client()
)
}
\arguments{
\item{view_type}{Qualifier for type of experiments to be returned. Defaults to \code{ACTIVE_ONLY}.}

\item{client}{an MLFlow client}
}
\value{
A character vector of experiment IDs without \code{NODELETE} flags
}
\description{
List experiments without \code{NODELETE} tags
}
