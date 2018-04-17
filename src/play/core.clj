(ns play
  (:require [clojure.set :as set]
            [ubergraph.core :as uber]
            [loom.alg :as alg]
            [hugsql.core :as hugsql]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all :as helpers]
            [honeysql-postgres.format :refer :all]
            [honeysql-postgres.helpers :refer :all]
            [clojure.java.jdbc :as j]

            [table-spec.core :as t]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            )
  (:import [com.opentable.db.postgres.embedded EmbeddedPostgres]))


(def pg (-> (EmbeddedPostgres/builder)
            .start))

(def subname (str "//localhost:" (.getPort pg) "/postgres"))

(def db-spec {:classname "org.postgresql.Driver"
              :subprotocol "postgresql"
              :subname subname
              :user "postgres"
              :sslfactory "org.postgresql.ssl.NonValidatingFactory"})

;; (def real-db {:dbtype "postgresql"
;;               :dbname "postgres"
;;               :host "127.0.0.1"
;;               :port "5439"
;;               :user "postgres"
;;               :sslfactory "org.postgresql.ssl.NonValidatingFactory"})

(hugsql/def-db-fns "db.sql")

(create-persons-table db-spec)
(create-dogs-table db-spec)

(def connection-uri (str "jdbc:postgresql://localhost:" (.getPort pg) "/postgres?user=postgres&password=secret"))

;; (def real-connection-uri (str "jdbc:postgresql://localhost:5439/postgres?user=postgres&password=secret"))

(-> {:connection-uri connection-uri :schema "public"}
    (t/tables)
    (t/register))

(get-fk-dependencies db-spec)

(defn create-insert
  [m {:keys [fk_table fk_column pk_table pk_column]}]
  (-> (insert-into fk_table)
      (values [(merge m {fk_column {:select [pk_column] :from [pk_table] :limit 1}})])))

;;(create-insert {:father 1 :name "joe"} {:fk_table :persons, :fk_column :father, :pk_table :persons, :pk_column :id})

;;=> {:insert-into :persons, :values [{:father {:select [:id], :from [:persons], :limit 1}}]}
;; working
(defn dfs
  ([n g] (dfs [n] #{} g))
  ([nxs v g]
   (let [n (peek nxs)
         v (conj v n)]
     (when n
       (cons n (dfs (filterv #(not (v %)) (concat (pop nxs) (n g))) v g))))))

;; (dfs :a {:a #{:b} :b #{:b}})

(def tables
  [{:fk_table "persons", :fk_column "father", :pk_table "persons", :pk_column "id"}
   {:fk_table "dogs", :fk_column "owner", :pk_table "persons", :pk_column "id"}])


(def tables-2
  [{:fk_table "persons", :fk_column "father", :pk_table "persons", :pk_column "id"}
   {:fk_table "dogs", :fk_column "owner", :pk_table "persons", :pk_column "id"}
   {:fk_table "dogs", :fk_column "home", :pk_table "address", :pk_column "id"}])

(def db
        {"persons" {"persons" {:fk_column "father", :pk_column "id"}},
         "dogs"    {"persons" {:fk_column "owner", :pk_column "id"},
                    "address" {:fk_column "home", :pk_column "id"}}
         "address" {}})

(defn table->db
  [table]
  (reduce
    (fn [coll {:keys [fk_table fk_column pk_table pk_column]}]
      (if (contains? coll fk_table)
        (update-in coll [fk_table] assoc pk_table {:fk_column fk_column :pk_column pk_column})
        (assoc coll fk_table {pk_table {:fk_column fk_column :pk_column pk_column}})))
    {} table))

(defn db->graph
  [db]
  (reduce-kv
   (fn [m k v]
     (assoc m k (into #{} (keys v))))
   {} db))

(defn keyify
  [coll]
  (map #(reduce-kv (fn [m k v] (assoc m k (keyword v))) {} %) coll))

;;(create-insert {:father 1 :name "joe"} {:fk_table :persons, :fk_column :father, :pk_table :persons, :pk_column :id})
;; (create-insert {:father 1 :name "joe"} {:fk_table :persons, :fk_column :father, :pk_table :persons, :pk_column :id})

(s/exercise :table/dogs)

;; used if we wanted to spec via qualified names
;; (defn qualifier [n] (keyword (-> *ns* ns-name str) (name n)))

(defn qualifier [n] (keyword (str "table/" (name n))))
(defn g [t] (first (gen/sample (s/gen (qualifier t)) 1)))


;;=> {:insert-into :persons, :values [{:father {:select [:id], :from [:persons], :limit 1}}]}


(defn create-insert
  [m {:keys [fk_table fk_column pk_table pk_column]}]
  (-> (insert-into fk_table)
      (values [(merge m {fk_column {:select [pk_column] :from [pk_table] :limit 1}})])))

(defn ->inserts
  [root tables]
  (let [db (->> tables keyify table->db)]
    (->> db
        db->graph
        (dfs root)
        (reduce (fn [c t]
                  (conj c
                        (map (fn [r] (create-insert (g t) r))
                             (t (group-by :fk_table (keyify tables))))))
                []))))

(->> (get-fk-dependencies db-spec)
     (->inserts :dogs)
     flatten
     (map sql/format)
     first)


