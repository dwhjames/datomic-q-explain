(ns datomic-q-explain.test-select-index
  (:require [clojure.test :refer :all]
            [datomic-q-explain.core :refer :all]
            [datomic.api :as d]))


(defn eval-where-clause
  "Evaluate a single where clause"
  [datoms-fn is-ref-attr? has-index? ctx clause]
  (let [[index components filter-fn] (compute-index-traversal is-ref-attr? has-index? ctx clause)
        ds (seq (apply datoms-fn index components))]
    (if filter-fn
      (filter filter-fn ds)
      ds)))


(deftest where-clause-eavt
  (let [test-datom-a {:e 1000 :a 10 :v "a" :tx 101 :added true}
        test-datom-b {:e 1000 :a 11 :v "b" :tx 101 :added true}
        test-datom-c {:e 1000 :a 11 :v "c" :tx 102 :added true}
        test-datoms [test-datom-a test-datom-b test-datom-c]

        is-ref-attr? (fn [_] false)
        has-index? (fn [_] false)]

    (testing "queries requiring the :eavt index"
      (let [test-datoms-fn-scan-e
            (fn [index e]
                 (is (= :eavt index))
                 (is (= 1000 e))
                 test-datoms)]

        (testing "scanning the entity"
         (let [res
               (eval-where-clause test-datoms-fn-scan-e
                                  is-ref-attr?
                                  has-index?
                                  '{?e 1000}
                                  '[?e ?a ?v])]
           (is (= 3
                  (count res)))
           (is (some #{test-datom-a} res))
           (is (some #{test-datom-b} res))
           (is (some #{test-datom-c} res))))

        (testing "scaning the entity and filtering by value"
          (let [res
                (eval-where-clause test-datoms-fn-scan-e
                                   is-ref-attr?
                                   has-index?
                                   '{?e 1000 ?v "a"}
                                   '[?e ?a ?v])]
            (is (= 1
                   (count res)))
            (is (= test-datom-a
                   (first res))))

          (let [res
                (eval-where-clause test-datoms-fn-scan-e
                                   is-ref-attr?
                                   has-index?
                                   '{?e 1000 ?v "d"}
                                   '[?e ?a ?v])]
            (is (empty? res))))

        (testing "scaning the entity and filtering by tx"
          (let [res
                (eval-where-clause test-datoms-fn-scan-e
                                   is-ref-attr?
                                   has-index?
                                   '{?e 1000 ?t 101}
                                   '[?e ?a ?v ?t])]
            (is (= 2
                  (count res)))
           (is (some #{test-datom-a} res))
           (is (some #{test-datom-b} res))))))))


(deftest where-clause-aevt
  (let [test-datom-a {:e 1000 :a 10 :v "a" :tx 101 :added true}
        test-datom-b {:e 1001 :a 10 :v "b" :tx 101 :added true}
        test-datom-c {:e 1001 :a 10 :v "a" :tx 102 :added true}
        test-datoms [test-datom-a test-datom-b test-datom-c]

        is-ref-attr? (fn [_] false)
        has-index? (fn [_] false)]

    (testing "queries requiring the :aevt index"
      (let [test-datoms-fn-scan-a
            (fn [index a]
              (is (= :aevt index))
              (is (= 10 a))
              test-datoms)]

        (testing "scanning the attribute"
         (let [res
               (eval-where-clause test-datoms-fn-scan-a
                                  is-ref-attr?
                                  has-index?
                                  '{?a 10}
                                  '[?e ?a ?v])]
           (is (= 3
                  (count res)))
           (is (some #{test-datom-a} res))
           (is (some #{test-datom-b} res))
           (is (some #{test-datom-c} res))))

        (testing "scaning the attribute and filtering by value"
          (let [res
                (eval-where-clause test-datoms-fn-scan-a
                                   is-ref-attr?
                                   has-index?
                                   '{?a 10 ?v "a"}
                                   '[?e ?a ?v])]
            (is (= 2
                   (count res)))
            (is (some #{test-datom-a} res))
            (is (some #{test-datom-c} res)))

          (is (empty? (eval-where-clause test-datoms-fn-scan-a
                                         is-ref-attr?
                                         has-index?
                                         '{?a 10 ?v "d"}
                                         '[?e ?a ?v]))))

        (testing "scaning the attribute and filtering by tx"
          (let [res
                (eval-where-clause test-datoms-fn-scan-a
                                   is-ref-attr?
                                   has-index?
                                   '{?a 10 ?t 101}
                                   '[?e ?a ?v ?t])]
            (is (= 2
                  (count res)))
            (is (some #{test-datom-a} res))
            (is (some #{test-datom-b} res))))

        (testing "scanning the attribute and filter by both value and tx"
          (let [res
                (eval-where-clause test-datoms-fn-scan-a
                                    is-ref-attr?
                                    has-index?
                                    '{?a 10 ?v "a" ?t 102}
                                    '[?e ?a ?v ?t])]
            (is (= 1
                   (count res)))
            (is (= test-datom-c
                   (first res)))))))))


(deftest where-clause-vaet
  (let [test-datom-a {:e 1000 :a 10 :v 1003 :tx 101 :added true}
        test-datom-b {:e 1001 :a 10 :v 1003 :tx 102 :added true}
        test-datoms [test-datom-a test-datom-b]

        is-ref-attr? (fn [_] true)
        has-index? (fn [_] false)]

    (testing "queries requiring the :vaet index"
      (let [test-datoms-fn
            (fn [index v a]
              (is (= :vaet index))
              (is (= 1003 v))
              (is (= 10 a))
              test-datoms)]

        (testing "lookup by attribute and value"
          (let [res
                (eval-where-clause test-datoms-fn
                                   is-ref-attr?
                                   has-index?
                                   '{?a 10 ?v 1003}
                                   '[?e ?a ?v])]
            (is (= 2
                   (count res)))
            (is (some #{test-datom-a} res))
            (is (some #{test-datom-b} res))))

        (testing "lookup by attribute and value, filtering by t"
          (let [res
                (eval-where-clause test-datoms-fn
                                   is-ref-attr?
                                   has-index?
                                   '{?a 10 ?v 1003 ?t 101}
                                   '[?e ?a ?v ?t])]
            (is (= 1
                   (count res)))
            (is (some #{test-datom-a} res)))))

      (let [test-datoms-fn
            (fn [index v]
              (is (= :vaet index))
              (is (= 1003 v))
              test-datoms)]

        (testing "lookup by value"
          (let [res
                (eval-where-clause test-datoms-fn
                                   is-ref-attr?
                                   has-index?
                                   '{?v 1003}
                                   '[?e ?a ?v])]
            (is (= 2
                   (count res)))
            (is (some #{test-datom-a} res))
            (is (some #{test-datom-b} res))))

        (testing "lookup by value, filtering by t"
          (let [res
                (eval-where-clause test-datoms-fn
                                   is-ref-attr?
                                   has-index?
                                   '{?v 1003 ?t 101}
                                   '[?e ?a ?v ?t])]
            (is (= 1
                   (count res)))
            (is (some #{test-datom-a} res))))))))

(deftest where-clause-avet
  (let [test-datom-a {:e 1000 :a 10 :v "a" :tx 101 :added true}
        test-datom-b {:e 1001 :a 10 :v "a" :tx 102 :added true}
        test-datom-c {:e 1002 :a 10 :v "b" :tx 103 :added true}

        is-ref-attr? (fn [_] false)
        has-index? (fn [_] true)]

    (testing "lookups requiring the :avet index"
      (let [test-datoms-fn
            (fn [index a v]
              (is (= :avet index))
              (is (= 10 a))
              (is (= "a" v))
              [test-datom-a test-datom-b])]

        (testing "lookup by attribute and value"
          (let [res
                (eval-where-clause test-datoms-fn
                                   is-ref-attr?
                                   has-index?
                                   '{?a 10 ?v "a"}
                                   '[?e ?a ?v])]
            (is (= 2
                   (count res)))
            (is (some #{test-datom-a} res))
            (is (some #{test-datom-b} res))))

        (testing "lookup by attribute and value, filtering by t"
          (let [res
                (eval-where-clause test-datoms-fn
                                   is-ref-attr?
                                   has-index?
                                   '{?a 10 ?v "a" ?t 101}
                                   '[?e ?a ?v ?t])]
            (is (= 1
                   (count res)))
            (is (some #{test-datom-a} res))))))))




(deftest where-clause-with-blank-value
  (let [test-datom-a {:e 1 :a 0 :v "a" :tx 123 :added true}
        test-datom-b {:e 1 :a 0 :v "b" :tx 124 :added true}
        test-datoms [test-datom-a test-datom-b]

        is-ref-attr? (fn [_] false)
        has-index? (fn [_] false)]

    (testing ":where clause (size 2) evaluation"

      (testing "with empty context"
        (let [test-datoms-fn
              (fn [& _]
                (throw (Exception. "should not have been invoked")))]
          (is (thrown? IllegalArgumentException
                       (eval-where-clause test-datoms-fn
                                          is-ref-attr?
                                          has-index?
                                          {}
                                          '[?e ?a])))))

      (testing "with entity position bound"
        (let [test-datoms-fn
              (fn [index e]
                (is (= :eavt index))
                (is (= 1 e))
                test-datoms)
              res
              (eval-where-clause test-datoms-fn
                                 is-ref-attr?
                                 has-index?
                                 '{?e 1}
                                 '[?e ?a])]
          (is (= 2
                 (count res)))
          (is (some #{test-datom-a} res))
          (is (some #{test-datom-b} res)))

        (let [test-datoms-fn
              (fn [index e]
                (is (= :eavt index))
                (is (= 1 e))
                '())]
          (is (empty? (eval-where-clause test-datoms-fn
                                         is-ref-attr?
                                         has-index?
                                         '{?e 1}
                                         '[?e ?a])))))

      (testing "with attribute position bound"
        (let [test-datoms-fn
              (fn [index a]
                (is (= :aevt index))
                (is (= 0 a))
                test-datoms)
              res
              (eval-where-clause test-datoms-fn
                                 is-ref-attr?
                                 has-index?
                                 '{?a 0}
                                 '[?e ?a])]
          (is (= 2
                 (count res)))
          (is (some #{test-datom-a} res))
          (is (some #{test-datom-b} res)))

        (let [test-datoms-fn
              (fn [index a]
                (is (= :aevt index))
                (is (= 0 a))
                '())]
          (is (empty? (eval-where-clause test-datoms-fn
                                         is-ref-attr?
                                         has-index?
                                         '{?a 0}
                                         '[?e ?a])))))

      (testing "with entity and attribute positions bound"
        (let [test-datoms-fn
              (fn [index e a]
                (is (= :eavt index))
                (is (= 1 e))
                (is (= 0 a))
                test-datoms)
              res
              (eval-where-clause test-datoms-fn
                                 is-ref-attr?
                                 has-index?
                                 '{?e 1 ?a 0}
                                 '[?e ?a])]
          (is (= 2
                 (count res)))
          (is (some #{test-datom-a} res))
          (is (some #{test-datom-b} res)))

        (let [test-datoms-fn
              (fn [index e a]
                (is (= :eavt index))
                (is (= 1 e))
                (is (= 0 a))
                '())]
          (is (empty? (eval-where-clause test-datoms-fn
                                         is-ref-attr?
                                         has-index?
                                         '{?e 1 ?a 0}
                                         '[?e ?a]))))))))
