# datomic-q-explain [![Build Status](https://travis-ci.org/dwhjames/datomic-q-explain.svg?branch=master)](https://travis-ci.org/dwhjames/datomic-q-explain)

A Clojure library to explain the consumption of datoms during query
evaluation. It provides a drop-in replacement for
[Datomic](http://www.datomic.com/)'s `q` function called `q-explain`,
which returns an explanation of the query, rather than the original
query result. The explanation is a breakdown of the number of datoms
consumed by each `:where` clause and the index these datoms were drawn
from.

Some examples using the
[Datomic mbrainz example database](https://github.com/Datomic/mbrainz-sample)
and the
[sample queries](https://github.com/Datomic/mbrainz-sample/wiki/Queries)
are available in [mbrainz.clj](examples/mbrainz.clj) in the
[examples folder](examples).

This library is still in early development; see the list of
outstanding [TODOs](TODO.md) for current limitations and future plans.

## License

Copyright © 2014 Daniel W. H. James

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
