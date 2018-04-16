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
            [clojure.spec.alpha :as s])
  (:import [com.opentable.db.postgres.embedded EmbeddedPostgres]))


(def pg (-> (EmbeddedPostgres/builder)
            .start))

(def subname (str "//localhost:" (.getPort pg) "/postgres"))

(def db-spec {:classname "org.postgresql.Driver"
              :subprotocol "postgresql"
              :subname subname
              :user "postgres"
              :sslfactory "org.postgresql.ssl.NonValidatingFactory"})


;; (def db {:dbtype "postgresql"
;;          :dbname "db"
;;          :host "127.0.0.1"
;;          :port "5439"
;;          :user "postgres"
;;          :sslfactory "org.postgresql.ssl.NonValidatingFactory"})

(hugsql/def-db-fns "db.sql")

(.getPort pg)


(create-persons-table db-spec)

(def connection-uri (str "jdbc:postgresql://localhost:" (.getPort pg) "/postgres?user=postgres&password=secret"))

(-> {:connection-uri connection-uri :schema "public"}
    (t/tables)
    (t/register))


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

;; (reduce
;;  (fn [coll {:keys [fk_table fk_column pk_table pk_column]}]
;;    (if (contains? coll fk_table)
;;      (update-in coll [fk_table] assoc pk_table {:fk_column fk_column :pk_column pk_column})
;;      (assoc coll fk_table {pk_table {:fk_column fk_column :pk_column pk_column}})))
;;  {} tables-2)



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

(s/def ::id int?)
(s/def ::father int?)
(s/def ::name string?)
(s/def ::owner ::id)

(s/def ::persons (s/keys :req-un [::id ::father ::name]))
(s/def ::dogs (s/keys :req-un [::id ::owner ::name]))


(defn qualifier [n] (keyword (-> *ns* ns-name str) (name n)))
;; (defn g [t] (first (gen/sample (s/gen (qualifier t)) 1)))




;;=> {:insert-into :persons, :values [{:father {:select [:id], :from [:persons], :limit 1}}]}


(defn ->inserts
  [tables]
  (let [db (->> tables keyify table->db)]
    (->> db
        db->graph
        (dfs :dogs)
        (reduce (fn [c t]
                  (conj c
                        (map (fn [r] (create-insert (g t) r))
                             (t (group-by :fk_table (keyify tables))))))
                []))))


;; (->> tables
;;      ->inserts
;;      flatten
;;      (map sql/format)
;;      first)



"INSERT INTO dogs (id, owner, name)
           VALUES (?, (SELECT id FROM persons LIMIT ?), ?) NULL"

 ;; (({:insert-into :dogs, :values [({:owner {:select [:id], :from [:persons], :limit 1}} {:id 0, :owner -1, :name ""})]}) ({:insert-into :persons, :values [({:father {:select [:id], :from [:persons], :limit 1}} {:id -1, :father -1, :name ""})]})))

;; Example of insert with select
;; INSERT into foo_bar (foo_id, bar_id)
;; VALUES ((select id from foo where name = 'selena'),
;;          (select id from bar where type = 'name'));    

;;Example of how to insert with select
;; (-> (insert-into :dogs)
;;     (values [{:id {:select [:id] :from [:persons] :limit 1}}])
;;     sql/format)



(defn create-insert
  [m {:keys [fk_table fk_column pk_table pk_column]}]
  (-> (insert-into fk_table)
      (values [(merge m {fk_column {:select [pk_column] :from [pk_table] :limit 1}})])))

src:/Users/drewverlee/.m2/repository/org/clojure/data.json/0.2.6/data.json-0.2.6.jar:/Users/drewverlee/.m2/repository/org/clojure/clojure/1.9.0/clojure-1.9.0.jar:/Users/drewverlee/.m2/repository/commons-codec/commons-codec/1.10/commons-codec-1.10.jar:/Users/drewverlee/.m2/repository/org/apache/commons/commons-compress/1.11/commons-compress-1.11.jar:/Users/drewverlee/.m2/repository/org/apache/commons/commons-lang3/3.4/commons-lang3-3.4.jar:/Users/drewverlee/.m2/repository/org/clojure/core.specs.alpha/0.1.24/core.specs.alpha-0.1.24.jar:/Users/drewverlee/.m2/repository/org/tukaani/xz/1.5/xz-1.5.jar:/Users/drewverlee/.m2/repository/org/clojure/spec.alpha/0.1.143/spec.alpha-0.1.143.jar:/Users/drewverlee/.m2/repository/tailrecursion/cljs-priority-map/1.2.0/cljs-priority-map-1.2.0.jar:/Users/drewverlee/.m2/repository/com/layerware/hugsql-adapter/0.4.8/hugsql-adapter-0.4.8.jar:/Users/drewverlee/.m2/repository/com/layerware/hugsql-core/0.4.8/hugsql-core-0.4.8.jar:/Users/drewverlee/.m2/repository/org/clojure/google-closure-library/0.0-20151016-61277aea/google-closure-library-0.0-20151016-61277aea.jar:/Users/drewverlee/.m2/repository/org/clojure/clojurescript/1.7.170/clojurescript-1.7.170.jar:/Users/drewverlee/.m2/repository/honeysql/honeysql/0.6.3/honeysql-0.6.3.jar:/Users/drewverlee/.m2/repository/commons-io/commons-io/2.4/commons-io-2.4.jar:/Users/drewverlee/.m2/repository/aysylu/loom/1.0.1/loom-1.0.1.jar:/Users/drewverlee/.m2/repository/org/postgresql/postgresql/42.2.2/postgresql-42.2.2.jar:/Users/drewverlee/.m2/repository/nilenso/honeysql-postgres/0.2.3/honeysql-postgres-0.2.3.jar:/Users/drewverlee/.m2/repository/clj-tuple/clj-tuple/0.2.2/clj-tuple-0.2.2.jar:/Users/drewverlee/.m2/repository/com/layerware/hugsql-adapter-clojure-java-jdbc/0.4.8/hugsql-adapter-clojure-java-jdbc-0.4.8.jar:/Users/drewverlee/.m2/repository/org/mozilla/rhino/1.7R5/rhino-1.7R5.jar:/Users/drewverlee/.m2/repository/com/layerware/hugsql/0.4.8/hugsql-0.4.8.jar:/Users/drewverlee/.m2/repository/org/clojure/google-closure-library-third-party/0.0-20151016-61277aea/google-closure-library-third-party-0.0-20151016-61277aea.jar:/Users/drewverlee/.m2/repository/riddley/riddley/0.1.12/riddley-0.1.12.jar:/Users/drewverlee/.m2/repository/com/google/guava/guava/18.0/guava-18.0.jar:/Users/drewverlee/.m2/repository/org/clojure/java.jdbc/0.7.5/java.jdbc-0.7.5.jar:/Users/drewverlee/.m2/repository/com/google/javascript/closure-compiler/v20151015/closure-compiler-v20151015.jar:/Users/drewverlee/.m2/repository/com/opentable/components/otj-pg-embedded/0.7.1/otj-pg-embedded-0.7.1.jar:/Users/drewverlee/personal/postgres-mock/playgroud/../table-spec/src:/Users/drewverlee/.m2/repository/org/clojure/tools.reader/1.1.0/tools.reader-1.1.0.jar:/Users/drewverlee/.m2/repository/potemkin/potemkin/0.4.3/potemkin-0.4.3.jar:/Users/drewverlee/.m2/repository/org/slf4j/slf4j-api/1.7.12/slf4j-api-1.7.12.jar:/Users/drewverlee/.m2/repository/dorothy/dorothy/0.0.6/dorothy-0.0.6.jar:/Users/drewverlee/.m2/repository/ubergraph/ubergraph/0.5.0/ubergraph-0.5.0.jar:/Users/drewverlee/.m2/repository/org/clojure/test.check/0.9.0/test.check-0.9.0.jar:/Users/drewverlee/.m2/repository/org/clojure/data.priority-map/0.0.8/data.priority-map-0.0.8.jar
