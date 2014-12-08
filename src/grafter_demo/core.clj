(ns grafter-demo.core
  (:require [grafter.tabular :refer [graph-fn defpipeline derive-column mapc drop-rows add-columns melt]]
            [grafter.tabular.common :refer [open-all-datasets make-dataset]]
            [grafter.rdf.protocols :as pr]
            [grafter.rdf.io :as io]
            [grafter.rdf.repository :refer :all]
            [grafter.rdf.templater :refer [graph]]
            [grafter.rdf.ontologies.rdf :refer :all]
            [grafter.rdf.ontologies.vcard :refer :all]
            [grafter.rdf.ontologies.dcterms :refer :all]
            [grafter.rdf.ontologies.qb :refer :all]
            [grafter.rdf.protocols :as pr]
            [grafter.rdf.ontologies.util :refer [prefixer]]
            [grafter-demo.util :refer :all]
            [clojure.string :as st])
  (:gen-class))

;;
;;; Bases URI & prefixers
;;

(def base-uri "http://opendatacommunities.org/id/housing/net-additional-dwellings/")
(def net-additional-dwellings-def "http://opendatacommunities.org/def/housing/netAdditionalDwellings")
(def gov-year (prefixer "http://reference.data.gov.uk/id/government-year/"))

(def sdmx-dimension (prefixer "http://purl.org/linked-data/sdmx/2009/dimension#"))
(def sdmx-dimension:refArea (sdmx-dimension "refArea"))
(def sdmx-dimension:refPeriod (sdmx-dimension "refPeriod"))

;;
;;; Data cleaning functions
;;

(defn rdfstr
  [str]
  (if (empty? str)
    ""
    (io/s str :en)))

(defn get-area-code
  "takes an uri like 'http://statistics.data.gov.uk/id/local-authority-district/45UB' and returns '45UB'"
  [string]
  (last (st/split string #"/")))

(defn net-additional-dwellings-id
  "returns the URI: 'http://opendatacommunities.org/id/housing/net-additional-dwellings/2008-2009/45UE'
  if obs is missing: returns nil"
  [period area-code obs]
  (if-not (empty? obs)
    (str base-uri period "/" area-code)
    ""))

;;
;;; Work on the tabular file
;;

(defn net-additional-dwellings-ppln
  "pipeline for 'net-additional-dwellings.csv'"
  [dataset]
  (-> dataset
      (make-dataset [:area :name "2000-2001" "2001-2002" "2002-2003" "2003-2004" "2004-2005" "2005-2006" "2006-2007" "2007-2008" "2008-2009" "2009-2010" "2010-2011"])
      (drop-rows 1)
      (melt :area :name)
      (derive-column :area-code [:area] get-area-code)
      (derive-column :net-additional-dwellings-uri [:variable :area-code :value] net-additional-dwellings-id)))

;;
;;; Create the template 
;;

(def net-additional-dwellings-template
  "RDF template for 'net-additional-dwellings.csv'"
  (graph-fn [{:keys [area name area-code net-additional-dwellings-uri variable value]}]

            (graph "http://opendatacommunities.org/graph/net-additional-dwellings"
                   [net-additional-dwellings-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, " variable ", " name))]
                    [net-additional-dwellings-def (parseValue value)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year variable)]])))

;;
;;; Filter the triples with missing data
;;

(defn net-additional-dwellings-filter
  "filter for 'net-additional-dwellings.csv'"
  [triples]
  (filter #(not (blank? (pr/subject %1))) triples))

;;
;;; Final pipeline
;;

(defpipeline net-additional-dwellings-pipeline
  [path output]
  (-> (open-all-datasets path)
      first
      net-additional-dwellings-ppln
      net-additional-dwellings-template
      (import-rdf output 
                  net-additional-dwellings-filter))
  (println "Grafted: " path))

;;
;;; Main function to call the pipeline from the command line
;;

(defn -main
  [& [path output]]
  (when-not (and path output)
    (println "Usage: lein run <input-file.csv> <output-file.(nt|rdf|n3|ttl)>")
    (System/exit 0))
  (net-additional-dwellings-pipeline path output))
