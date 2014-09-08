(ns datomic-q-explain.core-test
  (:require [clojure.test :refer :all]
            [datomic-q-explain.core :refer :all]
            [datomic.api :as d]))


(deftest attribute-predicate-tests
  (let [uri "datomic:mem://attribute-predicate-tests"]
    (d/create-database uri)
    (try
      (let [conn (d/connect uri)
            db (d/db conn)]

        (testing "is-ref-attr?"
          (is (= true
                 (is-ref-attr? db :db.install/attribute)))
          (is (= false
                 (is-ref-attr? db :db/doc))))

        (testing "has-index?"
          (is (= true
                 (boolean (has-index? db :db/ident))))
          (is (= false
                 (boolean (has-index? db :db/doc))))))
      (finally
        (d/delete-database uri)))))


(deftest bind-datoms-test
  (testing "bind :e :a :v and :tx"
    (let [res
          (bind-datoms {}
                       '[?e ?a ?v ?tx]
                       [{:e 1 :a 0 :v "a" :tx 123 :added true}])]
      (is (= 1 (count res)))
      (let [ctx (first res)]
        (is (= 1
               (get ctx '?e)))
        (is (= 0
               (get ctx '?a)))
        (is (= "a"
               (get ctx '?v)))
        (is (= 123
               (get ctx '?tx))))))

  (testing "don't bind blank"
    (let [res
          (bind-datoms {}
                       '[?e ?a _ ?tx]
                       [{:e 1 :a 0 :v "a" :tx 123 :added true}])]
      (is (= 1 (count res)))
      (let [ctx (first res)]
        (is (= 3
               (count ctx)))
        (is (= 1
               (get ctx '?e)))
        (is (= 0
               (get ctx '?a)))
        (is (= 123
               (get ctx '?tx))))))

  (testing "skip already bound vars"
    (let [res
          (bind-datoms '{?v "b"}
                       '[?e ?a ?v ?tx]
                       [{:e 1 :a 0 :v "a" :tx 123 :added true}])]
      (is (= 1 (count res)))
      (let [ctx (first res)]
        (is (= 1
               (get ctx '?e)))
        (is (= 0
               (get ctx '?a)))
        (is (= "b"
               (get ctx '?v)))
        (is (= 123
               (get ctx '?tx))))))

  (testing "bind many datoms"
    (let [res
          (bind-datoms '{?e 10}
                       '[?e ?a]
                       [{:e 1 :a 0 :v "a" :tx 123 :added true}
                        {:e 1 :a 1 :v "a" :tx 123 :added true}
                        {:e 1 :a 2 :v "a" :tx 123 :added true}
                        {:e 1 :a 3 :v "a" :tx 123 :added true}])]
      (is (= 4 (count res)))
      (is (= '{?e 10 ?a 0}
             (nth res 0)))
      (is (= '{?e 10 ?a 1}
             (nth res 1)))
      (is (= '{?e 10 ?a 2}
             (nth res 2)))
      (is (= '{?e 10 ?a 3}
             (nth res 3))))))


(deftest abstract-clause-test
  (testing "abstract nothing when all vars"
    (let [ctx {}
          clause '[?e ?a ?v]
          [ctx1 [lbl & clause1]] (abstract-clause ctx clause)]
      (is (= ctx ctx1))
      (is (= :data lbl))
      (is (= clause clause1))))

  (testing "don't abstract db vars"
    (let [ctx {}
          clause '[$ ?e ?a]
          [ctx1 [lbl & clause1]] (abstract-clause ctx clause)]
      (is (= ctx ctx1))
      (is (= :data lbl))
      (is (= clause clause1)))
    (let [ctx {}
          clause '[$db ?e ?a]
          [ctx1 [lbl & clause1]] (abstract-clause ctx clause)]
      (is (= ctx ctx1))
      (is (= :data lbl))
      (is (= clause clause1))))

  (testing "skip wildcard"
    (let [ctx {}
          clause '[?e _ ?v]
          [ctx1 [lbl & clause1]] (abstract-clause ctx clause)]
      (is (= ctx ctx1))
      (is (= :data lbl))
      (is (= clause clause1))))

  (testing "abstract attribute"
    (let [ctx {}
          clause '[?e :attr ?v]
          [ctx1 [lbl & clause1]] (abstract-clause ctx clause)]
      (is (= :data lbl))
      (is (= 1
             (count ctx1)))
      (is (some #{:attr}
                (vals ctx1)))
      (is (= :attr
             (get ctx1
                  (nth clause1 1)))))))
