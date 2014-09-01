(ns alt-q.core
  (:require [datomic.api :as d]))

(set! *warn-on-reflection* true)


(defn compute-index-traversal
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
  [ctx d f k & fks]
  (let [ret (assoc ctx k (f d))]
    (if fks
      (recur ret d (first fks) (second fks) (nnext fks))
      ret)))


;; TODO: ignoring :added for now
(defn bind-datoms
  ""
  [ctx clause ds]
  (when (seq ds)
    (let [bind-args
          (->> clause
               (map vector [:e :a :v :tx])
               (filter #(not (or (contains? ctx (second %))
                                 (= '_ (second %)))))
               (apply concat))]
      (if (seq bind-args)
        (map #(apply bind-query-vars ctx % bind-args) ds)
        (list ctx)))))


(defn abstract-clause
  [ctx clause]
  (loop [ctx ctx
         clause clause
         acc []]
    (if-let [x (first clause)]
      (if (and (symbol? x)
               (-> x name first #{\? \_ \$}))
        (recur ctx
               (rest clause)
               (conj acc x))
        (let [s (gensym "?")]
          (recur (assoc ctx s x)
                 (rest clause)
                 (conj acc s))))
      [ctx acc])))


(defn abstract-clauses
  [ctx clauses]
  (loop [ctx ctx
         clauses clauses
         acc []]
    (if (seq clauses)
      (let [[ctx1 clause1] (abstract-clause ctx (first clauses))]
        (recur ctx1 (rest clauses) (conj acc clause1)))
      [ctx acc])))


(defn lookup-db-for-clause
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
  [db attr]
  (-> db
      (d/entity attr)
      :db/valueType
      (= :db.type/ref)))


(defn has-index?
  [db attr]
  (let [e (d/entity db attr)]
    (or (:db/index e)
        (:db/unique e))))


(defn- one-step
  [ctx clause]
  (let [[db clause1]
        (lookup-db-for-clause ctx clause)
        [index components filter-fn]
        (compute-index-traversal (partial is-ref-attr? db)
                                 (partial has-index? db)
                                 ctx
                                 clause1)
        datoms (seq (apply d/datoms db index components))
        ctxs (bind-datoms ctx
                          clause1
                          (if filter-fn
                            (filter filter-fn datoms)
                            datoms))]
    [index ctxs]))


(defn count-query
  [ctx clauses]
  (let [cnts (-> clauses count (repeat {}) vec atom)
        [ctx1 clauses1] (abstract-clauses ctx clauses)        
        go (fn rec [ctx clauses i]
             (when (seq clauses)
               (let [cnt (atom 0)
                     [index ctxs] (one-step ctx (first clauses))]
                 (doseq [ctx1 ctxs]
                   (swap! cnt inc)
                   (rec ctx1 (rest clauses) (inc i)))
                 (swap! cnts
                        #(update-in % [i index]
                                    (fnil (partial + @cnt) 0))))))]
    (go ctx1 clauses1 0)
    (map vector clauses @cnts)))


(defn extract-query
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
  [query & args]
  (let [[in-vars clauses] (extract-query query)]
    (when (not= (count args) (count in-vars))
      (throw (IllegalArgumentException. (str "query expected "
                                             (count in-vars)
                                             " arguments, but given "
                                             (count args)))))
    (count-query (zipmap in-vars args)
                 clauses)))
