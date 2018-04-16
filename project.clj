(defproject play "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [ubergraph "0.5.0"]
                 [com.layerware/hugsql "0.4.8"]
                 [org.postgresql/postgresql "42.2.2"]
                 [com.opentable.components/otj-pg-embedded "0.7.1"]
                 [org.clojure/java.jdbc "0.7.5"]
                 [viesti/table-spec "0.1.1"]
                 [nilenso/honeysql-postgres "0.2.3"]
                 [org.clojure/test.check "0.9.0"]])
