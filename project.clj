(defproject grafter-demo "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [grafter "0.3.0-SNAPSHOT"]
                 [org.slf4j/slf4j-jdk14 "1.7.5"]]
  :aot [grafter-demo.core]
  :main grafter-demo.core)
