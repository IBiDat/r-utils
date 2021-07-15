# Copyright (c) 2021, Janko Thyson, Iñaki Úcar
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

# global repo for interfaces
if (is.null(getOption("solidr.repo")))
  options(solidr.repo=new.env())

#' Register specialized type for an interface
#'
#' @param cls specialized type
#' @param as interface name
#' @param inst optional instance
#' @param singleton whether to keep a single instance
#' @param overwrite whether to force registration
#' @param repo repository environment
#'
registerType <- function(cls, as, inst = NULL, singleton = FALSE,
                         overwrite = !is.null(inst), repo = getOption("solidr.repo")) {
  if (exists(as, envir=repo, inherits=FALSE) && !overwrite)
    stop(sprintf("Already registered: %s as %s", get(as, repo)$classname, as))

  if (is.character(cls))
    cls <- get(cls, parent.frame())
  if (!is.null(iface <- get0(as, parent.frame())))
    .checkInterfaceMatch(cls, iface)

  if (!singleton) {
    assign(as, cls, repo)
  } else {
    if (is.null(inst)) {
      attributes(cls)$singleton <- TRUE
      assign(as, cls, repo)
    } else {
      attributes(inst)$singleton <- TRUE
      assign(as, inst, repo)
    }
  }
}

# TODO: inherits = TRUE/FALSE? Some other restrictions necessary
# (e.g. `as.environment("package:xyz")`)?
.checkInterfaceMatch <- function(impl, ifac) {
  for (method in names(ifac$public_methods)) {
    if (method %in% c("clone", "initialize"))
      next

    # implements method
    if(!method %in% names(impl$public_methods))
      stop(sprintf("Method %s from interface %s is not implemented in %s",
                   method, ifac$classname, impl$classname))

    # method has same signature
    fmls_ifac <- formals(ifac$public_methods[[method]])
    fmls_impl <- formals(impl$public_methods[[method]])
    if(!identical(fmls_ifac, fmls_impl))
      stop(sprintf("Formals in method %s from %s don't match the interface %s",
                   method, impl$classname, ifac$classname))
  }
}

#' Resolve specialized type for an interface
#'
#' @param as interface name
#' @param repo repository environment
#'
resolveType <- function(as, repo = getOption("solidr.repo")) {
  gen <- repo[[as]]

  if (is.null(gen) || !inherits(gen, "R6ClassGenerator"))
    return(gen)

  gen <- .resolveTypeInner(gen, "public_fields", repo)
  gen <- .resolveTypeInner(gen, "private_fields", repo)
  inst <- gen$new()

  # initialize singleton
  if (isTRUE(attributes(gen)$singleton))
    registerType(gen, as, inst = inst, singleton = TRUE, repo = repo)

  inst
}

.resolveTypeInner <- function(gen, what, repo) {
  x <- gen[[what]]
  for (field in 1:length(x)) {
    value <- x[[field]]
    if (!inherits(value, "R6"))
      next
    if (!is.null(this <- resolveType(name = class(value)[1], repo = repo)))
      gen[[what]][[field]] <- this
  }
  gen
}

#' Define interface
#'
#' @param as interface name
#' @param methods list of methods
#' @param inherit another base class to inherit from
#' @param envir assignment environment
#'
defineType <- function(as, methods, inherit=NULL, envir=parent.frame()) {
  cls <- R6::R6Class(
    as,
    inherit = R6::R6Class(
      "IR6",
      inherit = inherit,
      private = list(
        model = NULL,
        service = list()
      )
    ),
    public = lapply(methods, function(args) {
      method <- function() stop("I'm an abstract interface method")
      formals(method) <- args
      method
    })
  )
  assign(as, cls, envir)
}
