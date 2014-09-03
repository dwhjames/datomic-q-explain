# datomic-q-explain

A Clojure library to explain the consumption of datoms during query
evaluation. It provides a drop-in replacement for Datomic's `q`
function called `q-explain`, which returns an explanation of the query,
rather than the original query result. The explanation is a breakdown
of the number of datoms consumed by each `:where` clause and index
these datoms were drawn from.


## License

Copyright © 2014 Daniel W. H. James

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
