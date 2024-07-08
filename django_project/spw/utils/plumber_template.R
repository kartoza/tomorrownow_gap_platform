# plumber.R

library(tidyverse)
library(tidygam)
library(mgcv)
library(ggpubr)

#* @plumber
function(pr) {
  err_func <- function(req, res, err) {
    print(err)
    res$status <- 500
    return (
      list(error = paste('Unhandled exception: ', err$message), detail = as.character(err))
    )
  }

  plumber::pr_set_error(pr, err_func)
}

#* Echo back the input
#* @get /statistical/echo
function() {
  list(msg = paste0("Plumber is working!"))
}
