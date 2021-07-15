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

http_call <- function(base_url, path, ssl_cert_file=NULL, ssl_cert_path=NULL) {
  httr::set_config(httr::config(forbid_reuse=1L))
  url <- paste0(base_url, "/", path)

  if (!is.null(ssl_cert_file) && file.exists(ssl_cert_file))
    httr::set_config(httr::config(cainfo=ssl_cert_file))
  if (!is.null(ssl_cert_path) && dir.exists(ssl_cert_path))
    httr::set_config(httr::config(capath=ssl_cert_path))

  function(
    endpoint, args, as = NULL,
    content_type = "application/rds", accept = "application/rds")
  {
    response <- if (missing(args)) httr::GET(
      file.path(url, endpoint),
      httr::accept(accept)
    ) else httr::POST(
      file.path(url, endpoint),
      body = switch(
        content_type,
        "application/rds" = serialize(args, NULL),
        stop("content_type '", content_type, "' not supported", call.=FALSE)
      ),
      httr::content_type(content_type),
      httr::accept(accept)
    )

    clen <- response$headers$`content-length`
    message(endpoint, ": response length: ", clen, " bytes")

    if (httr::status_code(response) != 200)
      stop(paste(httr::content(response), collapse="\n"), call.=FALSE)

    known <- c("text/html", "image/png")

    if (httr::http_type(response) %in% known)
      httr::content(response, as)
    else unserialize(response$content)
  }
}
