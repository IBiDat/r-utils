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

I18n <- R6::R6Class(
  "I18n",

  public = list(
    initialize = function(lang, path="locale", default="en_US") {
      ext <- "\\.json"
      files <- list.files(path, pattern=ext)

      langs <- lapply(files, function(file) {
        lang <- jsonlite::fromJSON(file.path(path, file))
        lang$short <- sub(ext, "", file)
        lang
      })
      private$langs <- setNames(langs, sapply(langs, "[[", "short"))

      if (!default %in% names(private$langs))
        stop("default language not found", call.=FALSE)

      names(private$langs)[names(private$langs) == default] <- "default"
      names(private$langs)[names(private$langs) == lang] <- "current"
    },
    translate = function(keyword) {
      msg <- private$langs$current$messages[[keyword]]
      if (is.null(msg))
        msg <- private$langs$default$messages[[keyword]]
      msg
    },
    current = function() {
      lang <- private$langs$current$short
      if (is.null(lang))
        lang <- private$langs$default$short
      lang
    },
    available = function() {
      short <- sapply(unname(private$langs), "[[", "short")
      name <- sapply(unname(private$langs), "[[", "name")
      setNames(short, name)
    }
  ),

  private = list(
    langs = NULL
  )
)
