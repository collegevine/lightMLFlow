#' Set Git tracking tags outside of an MLFlow Project
#'
#' This is a utility to make it easier to set tracking tags. Basically, it tricks
#' MLFlow into thinking the code is running from an MLFlow Project when it's not,
#' so you get nice Git tracking in the UI without needing an `MLProject` file or
#' to clone a repo to get the same benefits.
#'
#' This also makes it easier to have MLFlow play nicely with `renv`.
#'
#' @param run_id An MLFlow run ID. Auto-generated if missing
#' @param client An MLFlow client. Auto-generated if missing
#' @param git_repo_url A URL for the Git repo where the code is being run from
#' This is expected to be of the form `https://<<GITHUB_TOKEN>>@github.com/<<org>>/<<repo>>`
#' @param git_repo_subdir The subdirectory in the repo where your code lives
#' @param source_git_commit A git commit hash to tag a run with. Defaults to \code{git2r::revparse_single(".", revision = "HEAD")$sha}
#' @param source_git_branch The git branch the code is running on. Defaults to \code{system("git rev-parse --abbrev-ref HEAD", intern = TRUE)} (i.e. the current branch)
#' @param project_entrypoint An entrypoint for the project. Defaults to `"main"`
#' @param project_backend A backend for the project. Defaults to `"local"`
#'
#' @return Returns a list of tags for the `run_id`
#' @export
set_git_tracking_tags <- function(
  run_id,
  client,
  git_repo_url = Sys.getenv("GIT_REPO_URL"),
  git_repo_subdir,
  source_git_commit = system("git rev-parse HEAD", intern = TRUE),
  source_git_branch = system("git rev-parse --abbrev-ref HEAD", intern = TRUE),
  project_entrypoint = "main",
  project_backend = "local"
) {

  .args <- resolve_args(
    run_id = maybe_missing(run_id),
    client = maybe_missing(client)
  )

  source_name <- paste(
    git_repo_url,
    git_repo_subdir,
    sep = "#"
  )

  set_tag(
    key = "mlflow.source.git.repoURL",
    value = git_repo_url,
    run_id = .args$run_id,
    client = .args$client
  )

  set_tag(
    key = "mlflow.source.type",
    value = "PROJECT",
    run_id = .args$run_id,
    client = .args$client
  )

  set_tag(
    key = "mlflow.source.git.commit",
    value = source_git_commit,
    run_id = .args$run_id,
    client = .args$client
  )

  set_tag(
    key = "mlflow.source.git.branch",
    value = source_git_branch,
    run_id = .args$run_id,
    client = .args$client
  )

  set_tag(
    key = "mlflow.source.name",
    value = source_name,
    run_id = .args$run_id,
    client = .args$client
  )

  set_tag(
    "mlflow.project.entryPoint",
    project_entrypoint,
    run_id = .args$run_id,
    client = .args$client
  )

  set_tag(
    "mlflow.project.backend",
    project_backend,
    run_id = .args$run_id,
    client = .args$client
  )

  get_run(
    run_id = .args$run_id,
    client = .args$client
  )$tags[[1]]
}
