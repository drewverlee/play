(ns play.dev
  (:require [play.core :refer :all])
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



(insert-data-for-deps db-spec :dogs)


(count (get-doggies db-spec))
(count (get-persons db-spec))

;;TODO maybe should be called get fk relationships
(get-fk-dependencies db-spec)
