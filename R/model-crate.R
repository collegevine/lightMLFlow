#' @rdname save_model
#' @export
save_model.crate <- function(model, path, model_spec = list(), ...) {

  if (!exists(path)) {
    .path_created <- TRUE
    dir.create(path, showWarnings = FALSE)
  }

  serialized <- serialize(model, NULL)

  objname <- "crate.bin"

  saveRDS(
    serialized,
    file.path(path, objname)
  )

  model_spec$flavors <- append(
    model_spec$flavors,
    list(
      crate = list(
        version = "0.1.0",
        model = objname
      )
    )
  )

  model_spec$object_name <- objname

  write_model_spec(
    path,
    model_spec
  )

  if (isTRUE(.path_created)) {
    unlink(path, recursive = TRUE, force = TRUE)
  }

  model_spec
}

#' @export
load_flavor.mlflow_flavor_crate <- function(flavor, model_path) {
  unserialize(readRDS(file.path(model_path, "crate.bin")))
}
