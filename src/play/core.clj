(ns play.core
  (:require [clojure.set :as set]
            [hugsql.core :as hugsql]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all :as helpers]
            [honeysql-postgres.format :refer :all]
            [honeysql-postgres.helpers :refer :all]
            [clojure.java.jdbc :as j]
            [table-spec.core :as t]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))

(defn create-insert
  [m {:keys [fk_table fk_column pk_table pk_column]}]
  (-> (insert-into fk_table)
      (values [(merge m {fk_column {:select [pk_column] :from [pk_table] :limit 1}})])))

(defn graph->dfs-path
  ([n g] (dfs [n] #{} g))
  ([nxs v g]
   (let [n (peek nxs)
         v (conj v n)]
     (when n (cons n (dfs (filterv #(not (v %)) (concat (pop nxs) (n g))) v g))))))

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

(defn genarate [t] (last (gen/sample (s/gen (qualifier t)) 30)))

;;TODO needs re-organizing. The args seem odd.
(defn create-insert-stmt
  [table-values {:keys [fk_table fk_column pk_table pk_column]}]
  (let [select-any {(keyword (str (name fk_table) "/" (name fk_column))) {:select [pk_column] :from [pk_table] :limit 1}}]
    (-> (insert-into fk_table)
        (values [(merge table-values select-any)]))))

(defn walk-path-and-create-insert-stmts
  [tables path]
  (reduce (fn [c t]
            (conj c
                  (let [table->fk-deps (group-by :fk_table (keyify tables))
                        table-values (genarate t)]
                    (if-let [fk-deps (t table->fk-deps)]
                      (map (fn [fk-table] (create-insert-stmt table-values fk-table))
                           fk-deps)
                      (-> (insert-into t)
                          (values [table-values]))))))
          [] path))

;; feels odd how i'm passing around tables here
(defn create-insert-stmts
  [root tables]
  (let [db (->> tables keyify table->db)]
    (->> db
         db->graph
         (dfs root)
         (walk-path-and-create-insert-stmts tables)
         flatten
         reverse
         (map sql/format))))

(defn insert-data-for-deps!
  [db root]
  (->> (create-insert-stmts root (get-fk-dependencies db))
       (map #(j/execute! db %))))
