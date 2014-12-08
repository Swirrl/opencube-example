(ns grafter-demo.util
  (:require [grafter.tabular :refer :all]
            [grafter.rdf.protocols :as pr]
            [grafter.rdf.io :as io]
            [grafter.rdf.repository :refer :all]))

(defn blank? [v]
  (or (nil? v) (= "" v)))

(defn empty-filter
  "Basic template of filter"
  [triples]
  (filter #(not (and (#{}
                      (pr/predicate %1))
                     (blank? (pr/object %1)))) triples))

(defn import-rdf
  [quads-seq destination filter]
  (let [now (java.util.Date.)
        quads (->> quads-seq
                   filter
                   ;; (validate-triples (complement has-blank?))
                   )]
    (pr/add (io/rdf-serializer destination) quads)))

(defmulti parseValue class)
(defmethod parseValue nil                 [x] nil)
(defmethod parseValue java.lang.Character [x] (Character/getNumericValue x))
(defmethod parseValue java.lang.String    [x] (if (= "" x)
                                                nil
                                                (if (.contains x ".")
                                                  (Double/parseDouble x)
                                                  (Integer/parseInt x))))
(defmethod parseValue :default            [x] x)
