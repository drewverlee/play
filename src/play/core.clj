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

(hugsql/def-db-fns "db.sql")

(create-persons-table real-db)
(create-dogs-table real-db)

(def connection-uri (str "jdbc:postgresql://localhost:" (.getPort pg) "/postgres?user=postgres&password=secret"))
(def real-connection-uri (str "jdbc:postgresql://localhost:5439/postgres?user=postgres&password=secret"))

(-> {:connection-uri real-connection-uri :schema "public"}
    (t/tables)
    (t/register))


(defn create-insert
  [m {:keys [fk_table fk_column pk_table pk_column]}]
  (-> (insert-into fk_table)
      (values [(merge m {fk_column {:select [pk_column] :from [pk_table] :limit 1}})])))

;; working
(defn dfs
  ([n g] (dfs [n] #{} g))
  ([nxs v g]
   (let [n (peek nxs)
         v (conj v n)]
     (when n
       (cons n (dfs (filterv #(not (v %)) (concat (pop nxs) (n g))) v g))))))

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

(defn qualifier [n] (keyword (str "table/" (name n))))
(defn g [t] (first (gen/sample (s/gen (qualifier t)) 1)))

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
     (map #(j/execute! db-spec %)))

(get-doggies db-spec)
