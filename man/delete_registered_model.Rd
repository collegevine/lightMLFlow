% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/registry.R
\name{delete_registered_model}
\alias{delete_registered_model}
\title{Delete registered model}
\usage{
delete_registered_model(name, client = mlflow_client())
}
\arguments{
\item{name}{The name of the model to delete}

\item{client}{An MLFlow client. Will be auto-generated if omitted.}
}
\value{
No return value. Called for side effects.
}
\description{
Deletes an existing registered model by name
}
