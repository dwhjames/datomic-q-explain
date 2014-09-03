(ns alt-q.core
  (:require [datomic.api :as d]))

(set! *warn-on-reflection* true)


(defn compute-index-traversal
  "Determine which index to use and how to traverse it,
   given a query `:where` clause `clause` and
   an evaluation context `ctx`.

   The functions `is-ref-attr?` and `has-index?` are
   invoked to determine the `:vaet` and `:avet` indexes
   can be used, respectively.

   This function returns a triple, of the index keyword
   (one of `:eavt`, `:aevt`, `:avet`, or `:vaet`), the
   fixed components to apply to the index lookup, and
   an optional predicate that if non-nil should be used
   to filter the datoms that a returned from an index
   traversal.

   Example:
       (let [[index components filter-fn] (compute-index-traversal ...)
             ds (seq (apply d/datoms index components))]
         (if filter-fn (filter filter-fn ds) ds))
   "
  [is-ref-attr? has-index? ctx clause]
  (let [;; extract the symbols from the clause
        [e-sym a-sym v-sym t-sym] clause
        ;; look up symbols in context
        e (get ctx e-sym)
        a (get ctx a-sym)
        v (get ctx v-sym)
        t (get ctx t-sym)]
    (if e
     ;; ?e is bound
     (if a
       ;; ?e and ?a are bound
       (if v
         ;; ?e ?a and ?v are bound
         (if t
           ;; only ?e ?a ?v and ?t are bound
           [:eavt [e a v t] nil]
           ;; only ?e ?a and ?v are bound
           [:eavt [e a v] nil])
         ;; ?e and ?a are bound, not ?v
         (if t
           ;; ?e ?a and ?t are bound, not ?v
           [:eavt [e a] #(= t (:tx %))]
           ;; only ?e and ?a are bound
           [:eavt [e a] nil]))
       ;; ?e is bound and ?a is not bound
       (if v
         ;; ?e and ?v are bound, not ?a
         (if t
           ;; only ?e ?v and ?t are bound
           [:eavt [e]
            ;; filter for ?v and ?t
            #(and (= v (:v %))
                  (= t (:tx %)))]
           ;; only ?e and ?v are bound
           [:eavt [e]
            ;; filter for ?v
            #(= v (:v %))])
         ;; ?e is bound, not ?a or ?v
         (if t
           ;; only ?e and ?t are bound
           [:eavt [e]
            ;; filter for ?t
            #(= t (:tx %))]
           ;; only ?e is bound
           [:eavt [e] nil])))
     ;; ?e is not bound
     (if a
       ;; ?a is bound, not ?e
       (if v
         ;; ?a and ?v are bound
         (cond
          (is-ref-attr? a) ;; if attr is ref type
          (if t
            ;; only ?a ?v and ?t are bound
            [:vaet [v a]
             ;; filter for ?t
             #(= t (:tx %))]
            ;; only ?a and ?v are bound
            [:vaet [v a] nil])
          (has-index? a) ;; if attr has index
          (if t
            ;; only ?a ?v and ?t are bound
            [:avet [a v]
             ;; filter for t
             #(= t (:tx %))]
            ;; only ?a and ?v are bound
            [:avet [a v] nil])
          :else
          (if t
            ;; only ?a ?v and ?t are bound
            [:aevt [a]
             ;; filter for ?v and ?t
             #(and (= v (:v %))
                   (= t (:tx %)))]
            ;; only ?a and ?v are bound
            [:aevt [a]
             ;; filter for ?v
             #(= v (:v %))]))
         ;; ?a is bound, not ?e or ?v
         (if t
           ;; only ?a and ?t are bound
           [:aevt [a]
            #(= t (:tx %))]
           ;; only ?a is bound
           [:aevt [a] nil]))
       ;; neither ?e nor ?a are bound
       (throw (IllegalArgumentException. "not enough bound variables"))))))


(defn- bind-query-vars
  "Bind query variables to values extracted from a datom.

   Evaluation context `ctx` is extended with variables `k`
   bound to values extract by functions `f` from datom `d`."
  [ctx d f k & fks]
  (let [ret (assoc ctx k (f d))]
    (if fks
      (recur ret d (first fks) (second fks) (nnext fks))
      ret)))


(defn bind-datoms
  "Bind a collection datoms `ds` to the variables in a `:where`
   clause `clause`, producing a lazy sequence of new evaluation
   contexts extends from `ctx`.

   It is assumed that `clause` is already fully abstracted, so
   it consists only of variables and no literals. Values are
   only extracted for unbound variables (and wildcards are skipped).

   Note: the `:added` value of a datom is currently ignored.
   The rationale is that this can't have an impact of datoms
   consumption in a query."
  [ctx clause ds]
  (when (seq ds)
    (let [bind-args
          (->> clause
               (map vector [:e :a :v :tx]) ;; zip with keyword accessors
               (filter (fn [[a b]] ;; only keep pairs for unbound vars
                         (not (or (contains? ctx b)
                                  (= '_ b)))))
               (apply concat))]
      (if (seq bind-args) ;; shortcut traversal if there is nothing to bind
        (map #(apply bind-query-vars ctx % bind-args) ds)
        (list ctx)))))


(defn is-expression-clause?
  "A clause is an expression clause if the first
   element is a list."
  [clause]
  (-> clause first list?))


(defn is-rule-invocation?
  "A clause is a rule invocation if the first element
   is a symbolic name (not a query var or wildcard), or
   if the second is a symblic name and the first is a
   database variable (symbol beginning with `$`)."
  [clause]
  (let [[a b] clause]
    (and (symbol? a)
         (or (and (-> a name first (= \$))
                  (symbol? b)
                  (-> b name first #{\? \_} not))
             (-> a name first #{\? \_} not)))))


(defn abstract-with-fresh-vars
  "Extend the evaluation context `ctx` with new query vars
   for elements of `coll` that are not already query var,
   db vars, or wildcards. Return the new context and the
   abstracted collection."
  [ctx coll]
  (loop [ctx ctx
         coll coll
         acc []]
    (if-let [x (first coll)]
      (if (and (symbol? x)
               (-> x name first #{\? \_ \$}))
        (recur ctx
               (rest coll)
               (conj acc x))
        (let [s (gensym "?")]
          (recur (assoc ctx s x)
                 (rest coll)
                 (conj acc s))))
      [ctx acc])))


(defn abstract-clause
  "Extend the evaluation context `ctx` by abstracting the
   `:where` clause `clause`, returning a pair of the new
   context and the new clause.

   Expression clauses are ignored, and rule invocations
   are handled specially to avoid abstracting the rule name."
  [ctx clause]
  (cond
   (is-expression-clause? clause)
   [ctx clause]
   (is-rule-invocation? clause)
   (if (-> clause first name first (= \$))
     (let [[db-var rule-name & args] clause
           [ctx1 abstract-args] (abstract-with-fresh-vars ctx args)]
       [ctx1
        (cons db-var (cons rule-name abstract-args))])
     (let [[rule-name & args] clause
           [ctx1 abstract-args] (abstract-with-fresh-vars ctx args)]
       [ctx1 (cons rule-name abstract-args)]))
   :else
   (abstract-with-fresh-vars ctx clause)))


(defn abstract-clauses
  "Extends the evaluation context `ctx` by abstracting the
   `:where` clauses `clauses`, returning a pair of the new
   context and the new clauses."
  [ctx clauses]
  (loop [ctx ctx
         clauses clauses
         acc []]
    (if (seq clauses)
      (let [[ctx1 clause1] (abstract-clause ctx (first clauses))]
        (recur ctx1 (rest clauses) (conj acc clause1)))
      [ctx acc])))


(defn lookup-db-for-clause
  "Determine the appropriate database value to use for the
   given `clause`. If the clause's first element specifies
   a database variable, then that database is looked up in
   `ctx`, otherwise the default database value is used, the
   one bound to `$`."
  [ctx clause]
  (let [first-sym (first clause)
        get-db (fn [db-sym]
                    (if-let [db (get ctx db-sym)]
                      db
                      (throw (IllegalArgumentException.
                              (str "no database is bound for symbol " db-sym)))))]
    (if (and (symbol? first-sym)
             (-> first-sym
                 name
                 first
                 (= \$)))
      [(get-db first-sym) (subvec clause 1)]
      [(get-db '$) clause])))


(defn is-ref-attr?
  "Test if the given attribute `attr` has a value type of ref."
  [db attr]
  (-> db
      (d/entity attr)
      :db/valueType
      (= :db.type/ref)))


(defn has-index?
  "Test if the given attribute `attr` has either an index
   or a uniqueness constraint."
  [db attr]
  (let [e (d/entity db attr)]
    (or (:db/index e)
        (:db/unique e))))


(defn eval-expression-clause
  "Evaluate an expression `clause` according to `ctx`.

   If it is just a predicate clause, and evaluates to true,
   then the same context is returned. Otherwise it is a
   function clause and the context is returned with the
   output variable bound to the result of the function.

   Note: only scalar bindings are currently supported."
  [ctx clause]
  (let [expr (first clause)
        expr1 (map #(get ctx % %) expr)]
    (case (count clause)
      1 (when (eval expr1)
          (list ctx))
      2 (let [out (second clause)]
          (if (symbol? out)
            (list (assoc ctx out (eval expr1)))
            (throw (UnsupportedOperationException.
                    (str "clause "
                         clause
                         " has a non-scalar binding."))))))))


;; assume ctx has % for the rule set
(defn resolve-rule-invoc
  "Resolves a rule invocation `rule-invoc` according to `ctx`.
   This function returns a sequence of triples.

   The first element of a triple is the input context,
   the context that the rule-body should be evaluated under.
   This includes the database, bound to `$`, the rule set,
   bound to `%`, and any input parameters that were bound
   at invocation.

   The second element of a triple is the output context.
   This is a mapping from parameters of the rule head that
   where unbound at invocation, mapped to argument variables.
   The evaluation of the rule body will bind values to the
   parameter variables, which can then be lifted to values
   for the argument variables in the invocation (outer)
   context.

   The third element of a triple is the rule body.

   For example,
       ctx
         {'$ ...
          '% '[[(my-rule ?i ?o)
                [...]
                [...]]]
          '?my-in-var 10}
       rule-invoc
         '(my-rule ?my-in-var ?my-out-var)
   gives
       in-ctx
         {'$ ...
          '% '[[(my-rule ?i ?o)
                [...]
                [...]]]
          '?i 10}
       out-ctx
         '{?o ?my-out-var}
       rule-body
         [[...] [...]]
   "
  [ctx rule-invoc]
  (let [rules (get ctx '%) ;; lookup rules from context
        [db rule-name & rule-args]
        (if (-> rule-invoc first name first (= \$))
          ;; if first element is db var, look it up
          (cons (get ctx
                     (first rule-invoc))
                (rest rule-invoc))
          ;; otherwise use the default db var
          (cons (get ctx '$)
                rule-invoc))
        rule-defs
        (filter #(= rule-name (ffirst %)) rules)]
    (if rule-defs ;; check that some rule defs are actually found
      (for [[[_ & rule-params] & rule-body] rule-defs]
       (conj
        (reduce ;; build the input and output contexts
         (fn [[in-ctx out-ctx]
             [param arg]]
           (if-let [x (get ctx arg)]
             [(assoc in-ctx param x) out-ctx]
             [in-ctx (assoc out-ctx param arg)]))
         [{'$ db '% rules} ;; the input context starts with a db and rules
          {}]
         (zipmap rule-params rule-args))
        rule-body))
      (throw (IllegalArgumentException. (str "No rule definition found for "
                                             rule-name))))))


(defn trace-iterator
  "Trace an iterator by counting the number
   of elements consumed. `cnt` should be an atom
   containing an integer counter."
  [^Iterable iterable cnt]
  (let [iter (.iterator iterable)]
    (reify java.util.Iterator
      (hasNext [_]
        (.hasNext iter))
      (next [_]
        (swap! cnt inc)
        (.next iter))
      (remove [_]
        (throw (UnsupportedOperationException.))))))


;; declare to enable mutual recursion
(declare eval-rule-invoc)

(defn one-step-rule
  "Evaluate one step of a rule body, a single clause.
   This returns a lazy sequence of extended eval contexts.

   If the clause is a rule invocation, then the rule evaluation
   recurses."
  [ctx cnt clause]
  (cond
   (is-expression-clause? clause)
   (eval-expression-clause ctx clause)
   (is-rule-invocation? clause)
   (eval-rule-invoc ctx cnt clause)
   :else
   (let [db (get ctx '$) ;; this is the database for the entire rule eval
         [index components filter-fn]
         (compute-index-traversal (partial is-ref-attr? db)
                                  (partial has-index? db)
                                  ctx
                                  clause)
         datoms (->
                 (apply d/datoms db index components)
                 (trace-iterator cnt)
                 iterator-seq)]
     (bind-datoms ctx
                  clause
                  (if filter-fn
                    (filter filter-fn datoms)
                    datoms)))))


(defn eval-rule-invoc
  "Evaluate a rule invocation `rule-invoc` according to `ctx`.

   This produces a lazy sequence of all extended contexts
   produced, by evaluating all rule heads that match, potentially
   with recursion."
  [ctx cnt rule-invoc]
  (mapcat ;; mapcat over all rule invocation resolutions
   (fn [[in-ctx out-ctx rule-body]]
     (let [;; a fn to recursively evaluate the rule body
           go (fn rec [curr-ctx body-clauses]
                (if (seq body-clauses)
                  ;; if there more clauses
                  (->> (first body-clauses)
                       ;; evalute the first to a sequence of contexts
                       (one-step-rule curr-ctx cnt)
                       ;; and recursively eval the rest of the body
                       ;; for each of those contexts
                       (mapcat #(rec % (rest body-clauses))))
                  ;; else return the context as is
                  (list curr-ctx)))]
       (->>
        ;; prepare the rule body by abstract all the clauses
        (abstract-clauses in-ctx rule-body)
        ;; recursively eval
        (apply go)
        ;; extract the output variables into the original context
        (map
         (fn [rule-ctx]
           (into ctx  ;; augment original ctx
                 (reduce ;; with rule-ctx according to out-ctx
                  (fn [acc [param arg]]
                    (assoc acc arg (get rule-ctx param)))
                  {}
                  out-ctx)))))))
   (resolve-rule-invoc ctx rule-invoc)))


(defn one-step
  "Evaluate one step of a query `:where` `clause`
   according to `ctx`.

   Returns a pair of the raw index accessed as a
   sequence of extended evaluation contexts. If the
   clause is an expression, then `:expr` is returned
   instead of a raw index keyword, and similarly,
   `:rule` for rule invocations."
  [ctx cnt clause]
  (cond
   (is-expression-clause? clause)
   [:expr (eval-expression-clause ctx clause)]
   (is-rule-invocation? clause)
   [:rule (eval-rule-invoc ctx cnt clause)]
   :else
   (let [[db clause1]
         (lookup-db-for-clause ctx clause)
         [index components filter-fn]
         (compute-index-traversal (partial is-ref-attr? db)
                                  (partial has-index? db)
                                  ctx
                                  clause1)
         datoms (->
                 (apply d/datoms db index components)
                 (trace-iterator cnt)
                 iterator-seq)
         ctxs (bind-datoms ctx
                           clause1
                           (if filter-fn
                             (filter filter-fn datoms)
                             datoms))]
     [index ctxs])))


(defn count-datom-usage
  "Count the number of datoms accessed while evaluating
   `clauses` according to `ctx`.

   This returns a sequence of the clauses paired with the
   index that was used for evaluating the clause as well
   as the number of datoms retrieved from that index."
  [ctx clauses]
  (let [cnts (-> clauses count (repeat {}) vec atom)
        go (fn rec [i ctx clauses]
             (when (seq clauses)
               (let [cnt (atom 0) ;; a counter for the clause eval
                     [index ctxs] (one-step ctx cnt (first clauses))]
                 ;; force the eval of the rest using each of the
                 ;; eval ctxs produced from the first
                 (doseq [ctx1 ctxs]
                   (rec (inc i) ctx1 (rest clauses)))
                 ;; all datoms needed for the first will now have been
                 ;; consumed, so `cnt` holds the total
                 (swap! cnts
                        #(update-in % [i index]
                                    (fnil (partial + @cnt) 0))))))]
    (apply go 0 (abstract-clauses ctx clauses))
    (map vector clauses @cnts)))


(defn extract-query
  "Given a regular Datomic query, extract the `:in` and
   `:where` sections.

   Returns a pair of the `:in` params and the
   `:where` clauses.

   Note that this function will accept the query in
   either vector or map form."
  [query]
  (cond
   (vector? query)
   (let [q1 (if (some #{:in} query)
              (drop-while #(not= % :in) query)
              (into '[:in $]
                    (drop-while #(not= % :where) query)))
         [in-vars where-and-clauses] (split-with #(not= % :where) (rest q1))]
     [(or (seq in-vars)
          '($))
      (rest where-and-clauses)])
   (map? query)
   [(or (seq (:in query))
        '($))
    (:where query)]))


(defn explain-query
  "Explain a Datomic query by showing the indexes that
   were accessed and the number of datoms consumed.

   This function will receive the same arguments as
   Datomic's `q` function, but will return the query
   explanation rather than the result set."
  [query & args]
  (let [[in-vars clauses] (extract-query query)]
    (when (not= (count args) (count in-vars))
      ;; complain if param and arg arity don't align
      (throw (IllegalArgumentException. (str "query expected "
                                             (count in-vars)
                                             " arguments, but given "
                                             (count args)))))
    (count-datom-usage (zipmap in-vars args)
                       clauses)))
