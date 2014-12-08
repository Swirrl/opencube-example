(ns grafter-demo.core
  (:require [grafter.tabular :refer [graph-fn defpipeline derive-column mapc drop-rows add-columns]]
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
      (make-dataset [:area :name :period-00-01 :period-01-02 :period-02-03 :period-03-04 :period-04-05 :period-05-06 :period-06-07 :period-07-08 :period-08-09 :period-09-10 :period-10-11])
      (add-columns {:y-00-01 "2000-2001" :y-01-02 "2001-2002" :y-02-03 "2002-2003" :y-03-04 "2003-2004" :y-04-05 "2004-2005" :y-05-06 "2005-2006" :y-06-07 "2006-2007" :y-07-08 "2007-2008" :y-08-09 "2008-2009" :y-09-10 "2009-2010" :y-10-11 "2010-2011"})
      (drop-rows 1)
      (derive-column :area-code [:area] get-area-code)
      (derive-column :net-additional-dwellings-00-01-uri [:y-00-01 :area-code :period-00-01] net-additional-dwellings-id)
      (derive-column :net-additional-dwellings-01-02-uri [:y-01-02 :area-code :period-01-02] net-additional-dwellings-id)
      (derive-column :net-additional-dwellings-02-03-uri [:y-02-03 :area-code :period-02-03] net-additional-dwellings-id)
      (derive-column :net-additional-dwellings-03-04-uri [:y-03-04 :area-code :period-03-04] net-additional-dwellings-id)
      (derive-column :net-additional-dwellings-04-05-uri [:y-04-05 :area-code :period-04-05] net-additional-dwellings-id)
      (derive-column :net-additional-dwellings-05-06-uri [:y-05-06 :area-code :period-05-06] net-additional-dwellings-id)
      (derive-column :net-additional-dwellings-06-07-uri [:y-06-07 :area-code :period-06-07] net-additional-dwellings-id)
      (derive-column :net-additional-dwellings-07-08-uri [:y-07-08 :area-code :period-07-08] net-additional-dwellings-id)
      (derive-column :net-additional-dwellings-08-09-uri [:y-08-09 :area-code :period-08-09] net-additional-dwellings-id)
      (derive-column :net-additional-dwellings-09-10-uri [:y-09-10 :area-code :period-09-10] net-additional-dwellings-id)
      (derive-column :net-additional-dwellings-10-11-uri [:y-10-11 :area-code :period-10-11] net-additional-dwellings-id)))

;;
;;; Create the template 
;;

(def net-additional-dwellings-template
  "RDF template for 'net-additional-dwellings.csv'"
  (graph-fn [{:keys [area name period-00-01 period-01-02 period-02-03 period-03-04 period-04-05 period-05-06 period-06-07 period-07-08 period-08-09 period-09-10 period-10-11 net-additional-dwellings-00-01-uri net-additional-dwellings-01-02-uri net-additional-dwellings-02-03-uri net-additional-dwellings-03-04-uri net-additional-dwellings-04-05-uri net-additional-dwellings-05-06-uri net-additional-dwellings-06-07-uri net-additional-dwellings-07-08-uri net-additional-dwellings-08-09-uri net-additional-dwellings-09-10-uri net-additional-dwellings-10-11-uri]}]

            (graph "http://opendatacommunities.org/graph/net-additional-dwellings"
                   [net-additional-dwellings-00-01-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2000-2001, " name))]
                    [net-additional-dwellings-def (parseValue period-00-01)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2000-2001")]]

                   [net-additional-dwellings-01-02-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2001-2002, " name))]
                    [net-additional-dwellings-def (parseValue period-01-02)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2001-2002")]]

                   [net-additional-dwellings-02-03-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2002-2003, " name))]
                    [net-additional-dwellings-def (parseValue period-02-03)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2002-2003")]]

                   [net-additional-dwellings-03-04-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2003-2004, " name))]
                    [net-additional-dwellings-def (parseValue period-03-04)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2003-2004")]]

                   [net-additional-dwellings-04-05-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2004-2005, " name))]
                    [net-additional-dwellings-def (parseValue period-04-05)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2004-2005")]]

                   [net-additional-dwellings-05-06-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2005-2006, " name))]
                    [net-additional-dwellings-def (parseValue period-05-06)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2005-2006")]]

                   [net-additional-dwellings-06-07-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2006-2007, " name))]
                    [net-additional-dwellings-def (parseValue period-06-07)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2006-2007")]]

                   [net-additional-dwellings-07-08-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2007-2008, " name))]
                    [net-additional-dwellings-def (parseValue period-07-08)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2007-2008")]]

                   [net-additional-dwellings-08-09-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2008-2009, " name))]
                    [net-additional-dwellings-def (parseValue period-08-09)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2008-2009")]]

                   [net-additional-dwellings-09-10-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2009-2010, " name))]
                    [net-additional-dwellings-def (parseValue period-09-10)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2009-2010")]]

                   [net-additional-dwellings-10-11-uri
                    [rdf:a qb:Observation]
                    [rdfs:label  (rdfstr (str "Net additional dwellings, 2010-2011, " name))]
                    [net-additional-dwellings-def (parseValue period-10-11)]
                    [qb:dataSet "http://opendatacommunities.org/data/net-additional-dwellings"]
                    [sdmx-dimension:refArea (rdfstr area)]
                    [sdmx-dimension:refPeriod (gov-year "2010-2011")]])))

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
