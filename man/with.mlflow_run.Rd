% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{with.mlflow_run}
\alias{with.mlflow_run}
\title{A \code{with} wrapper for MLFlow runs}
\usage{
\method{with}{mlflow_run}(data, expr, ...)
}
\arguments{
\item{data}{data to use for constructing an environment. For the default with method this may be an environment, a list, a data frame, or an integer as in sys.call. For within, it can be a list or a data frame.}

\item{expr}{expression to evaluate; particularly for within() often a “compound” expression, i.e., of the form

{
a <- somefun()
b <- otherfun()
.....
rm(unused1, temp)
}}

\item{...}{Arguments to be passed to future methods}
}
\value{
No return value. Called for side effects
}
\description{
Adds some error handling on exit
}
