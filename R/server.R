mlflow_cli_param <- function(args, param, value) {
  if (!is.null(value)) {
    args <- c(
      args,
      param,
      value
    )
  }

  args
}

validate_server <- function(client) {
  wait_for(
    function() mlflow_rest("experiments", "list", client = client),
    getOption("mlflow.connect.wait", 10),
    getOption("mlflow.connect.sleep", 1)
  )
}
