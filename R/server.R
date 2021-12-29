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
    function() call_mlflow_api("experiments", "list", client = client),
    getOption("mlflow.connect.wait", 10),
    getOption("mlflow.connect.sleep", 1)
  )
}
