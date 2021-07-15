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

RedisCache <- R6::R6Class(
  "RedisCache",
  public = list(
    initialize = function(host, port, maxsize, maxobj, maxtime, policy, namespace=NULL) {
      private$r <- redux::hiredis(host=host, port=port)
      private$r$CONFIG_SET("maxmemory", maxsize)
      private$r$CONFIG_SET("maxmemory-policy", paste0("allkeys-", policy))
      private$maxobj <- maxobj
      private$maxtime <- maxtime
      private$namespace <- namespace
    },
    get = function(key) {
      key <- paste0(private$namespace, "-", key)
      s_value <- tryCatch(private$r$GET(key), error=function(x) {
        warning("RedisCache get: key '", key,"' error: ", x, call.=FALSE)
        NULL
      })
      if (is.null(s_value))
        return(structure(list(), class = "key_missing"))
      private$r$EXPIRE(key, private$maxtime)
      qs::qdeserialize(s_value)
    },
    set = function(key, value) {
      key <- paste0(private$namespace, "-", key)
      s_value <- qs::qserialize(value)
      if (length(s_value) > private$maxobj)
        message("RedisCache set: avoiding too big key '", key, "'")
      else tryCatch({
        private$r$SET(key, s_value)
        private$r$EXPIRE(key, private$maxtime)
      }, error=function(x) {
        stop("RedisCache set: key '", key, "' error: ", x, call.=FALSE)
      })
    },
    reset = function() {
      private$r$FLUSHDB()
    }
  ),
  private = list(
    r = NULL,
    maxobj = NULL,
    maxtime = NULL,
    namespace = NULL
  )
)

cache_wrap <- function(cache) {
  if (is.null(cache))
    cache <- list(
      get = function(key) structure(list(), class = "key_missing"),
      set = function(key, value) NULL,
      reset = function() NULL
    )
  structure(
    list(
      get = cache$get,
      exists = cache$exists,
      keys = cache$keys,
      remove = cache$remove,
      reset = cache$reset,
      prune = cache$prune,
      size = cache$size,
      set = function(key, value) {
        if (is.null(value) || inherits(value, "try-error"))
          return()
        cache$set(key, value)
      }
    ),
    class = c("cache_wrap", class(cache))
  )
}

cache_init <- function(cache_opt) {
  if (!is.null(getOption("memo.cache")))
    return()

  maxtime <- sub("min", "*60", cache_opt$maxtime)
  maxtime <- eval(parse(text=maxtime))

  options(memo.cache = cache_wrap(switch(
    cache_opt$type,

    none = NULL,

    memory = {
      maxsize <- sub("b", "", cache_opt$maxsize)
      maxsize <- sub("m", "*1024^2", maxsize)
      maxsize <- eval(parse(text=maxsize))

      cachem::cache_mem(
        max_size = maxsize,
        max_age = maxtime,
        evict = cache_opt$policy
      )
    },

    disk = {
      maxsize <- sub("b", "", cache_opt$maxsize)
      maxsize <- sub("m", "*1024^2", maxsize)
      maxsize <- eval(parse(text=maxsize))

      cachem::cache_disk(
        dir = NULL,
        max_size = maxsize,
        max_age = maxtime,
        evict = cache_opt$policy,
        destroy_on_finalize = FALSE,
        prune_rate = 20
      )
    },

    redis = {
      maxobj <- sub("b", "", cache_opt$maxobj)
      maxobj <- sub("m", "*1024^2", maxobj)
      maxobj <- eval(parse(text=maxobj))

      RedisCache$new(
        host = cache_opt$host,
        port = cache_opt$port,
        maxsize = cache_opt$maxsize,
        maxobj = maxobj,
        maxtime = maxtime,
        policy = cache_opt$policy,
        namespace = "memo"
      )
    },

    {
      stop("Cache type '", cache_opt$type, "' not supported", call.=FALSE)
    }
  )))
}

cache_reset <- function() {
  cache <- getOption("memo.cache")
  if(is.null(cache))
    return(FALSE)
  cache$reset()
}

Memo <- R6::R6Class(
  "Memo",
  public = list(
    initialize = function() {
      public.interface <- setdiff(ls(self), c("clone", "initialize"))
      methods <- Filter(function(n){is.function(self[[n]])}, public.interface)

      for (method in methods) {
        unlockBinding(method, self)
        self[[method]] <- memoise::memoise(
          self[[method]], cache=getOption("memo.cache"))
        lockBinding(method, self)
      }
      invisible(self)
    }
  )
)
