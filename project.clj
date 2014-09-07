(defproject datomic-q-explain "0.1.0-SNAPSHOT"
  :description "A query explainer for Datomic"
  :url "https://github.com/dwhjames/datomic-q-explain"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.datomic/datomic-free "0.9.4894"
                  :exclusions [org.clojure/clojure
                               org.slf4j/slf4j-log4j12
                               org.slf4j/slf4j-nop]]
                 [ch.qos.logback/logback-classic "1.1.2"]]
  :jvm-opts ["-Xms1g" "-Xmx1g"])
