# Copyright (c) 2021, Iñaki Úcar
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

McPool <- R6::R6Class(
  "McPool",
  public = list(
    initialize = function(name, connect, check, disconnect, timeout=300) {
      private$name <- name
      private$connect <- connect
      private$check <- check
      private$disconnect <- disconnect
      private$timeout <- timeout
      private$last_cleanup <- Sys.time()
      private$host <- connect()
    },
    seize = function() {
      id <- private$available()[1]
      if (is.na(id)) {
        id <- private$new_id()
        private$con[[id]] <- list(obj = NA)
      }
      private$con[[id]]$in_use <- TRUE
      private$con[[id]]$last_use <- Sys.time()
      id
    },
    release = function(id) {
      private$con[[id]]$in_use <- FALSE
      private$con[[id]]$last_use <- Sys.time()
      private$cleanup()
    },
    get = function(id) {
      if (missing(id)) {
        while (!private$check(private$host))
          private$host <- private$connect()
        return(private$host)
      }
      while (!private$check(private$con[[id]]$obj))
        private$con[[id]]$obj <- private$connect()
      private$con[[id]]$obj
    }
  ),
  private = list(
    name = NULL,
    connect = NULL,
    check = NULL,
    disconnect = NULL,
    timeout = NULL,
    last_cleanup = NULL,
    host = NULL,
    con = list(),
    id = 0L,
    new_id = function() {
      private$id <- private$id + 1
      as.character(private$id)
    },
    available = function() {
      if (!length(private$con))
        return(character(0))
      in_use <- sapply(private$con, function(x) x$in_use)
      names(in_use)[!in_use]
    },
    cleanup = function() {
      t <- Sys.time()
      if (t - private$last_cleanup < private$timeout)
        return()
      age <- t - sapply(private$con, function(x) x$last_use)
      old <- names(age)[age > private$timeout]
      ids <- intersect(private$available(), old)
      for (id in ids)
        private$disconnect(private$con[[id]]$obj)
      private$con[ids] <- NULL
      private$last_cleanup <- t
    }
  )
)

mcpool_register <- function(name, connect, check, disconnect, timeout=300) {
  stopifnot(is.function(connect), is.function(check), is.function(disconnect))
  stopifnot(length(formals(connect)) == 0)
  stopifnot(length(formals(check)) == 1)
  stopifnot(length(formals(disconnect)) == 1)

  pools <- getOption("mcpool.pools")
  pools[[name]] <- McPool$new(name, connect, check, disconnect, timeout)
  options(mcpool.pools = pools)
}

mcpool_get_connection <- function(name) {
  if (!is.null(con <- getOption("mcpool.con")[[name]]))
    return(con) # worker
  pool <- getOption("mcpool.pools")[[name]]
  pool$get()    # host
}

future_promise <- function(
  expr=NULL, envir=parent.frame(), substitute=TRUE, globals=TRUE,
  packages=NULL, ..., queue=promises::future_promise_queue())
{
  if (requireNamespace("future", quietly=TRUE)) {
    if (!inherits(future::plan(), c("sequential", "multicore")))
      stop("mcpool can only be used in multicore setups", call.=FALSE)
  }

  pools <- getOption("mcpool.pools")
  envir <- new.env(parent=envir)
  envir$.pools <- pools
  envir$.ids <- lapply(pools, function(pool) pool$seize())

  init <- quote({
    options(mcpool.con = Map(function(pool, id) pool$get(id), .pools, .ids))
  })

  if (substitute)
    expr <- substitute(expr)
  expr <- as.call(append(as.list(init), expr, after=2))

  res <- promises::future_promise(
    expr=expr, envir=envir, substitute=FALSE, globals=globals,
    packages=packages, ..., queue=queue)

  promises::then(res, function(x) {
    Map(function(pool, id) pool$release(id), pools, envir$.ids)
    x
  })
}
