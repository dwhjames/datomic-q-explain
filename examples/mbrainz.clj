(require '[datomic.api :as d])
(require '[datomic-q-explain.core :as explain])

(def uri "datomic:free://localhost:4334/mbrainz")
(def conn (d/connect uri))

(def db (d/db conn))


;; Queries taken from
;; https://github.com/Datomic/mbrainz-sample/wiki/Queries


(d/q
 '[:find ?id ?type ?gender
   :in $ ?name
   :where
   [?e :artist/name ?name]
   [?e :artist/gid ?id]
   [?e :artist/type ?type]
   [?e :artist/gender ?gender]]
 db "Lupe Fiasco")


(explain/q-explain
 '[:find ?id ?type ?gender
   :in $ ?name
   :where
   [?e :artist/name ?name]
   [?e :artist/gid ?id]
   [?e :artist/type ?type]
   [?e :artist/gender ?gender]]
 db
 "Lupe Fiasco")


(d/q
 '[:find (count ?title)
   :in $ ?artist-name
   :where
   [?a :artist/name ?artist-name]
   [?t :track/artists ?a]
   [?t :track/name ?title]]
 db "John Lennon")

(explain/q-explain
 '[:find (count ?title)
   :in $ ?artist-name
   :where
   [?a :artist/name ?artist-name]
   [?t :track/artists ?a]
   [?t :track/name ?title]]
 db "John Lennon")


(count
 (d/q
  '[:find ?title ?album ?year
    :in $ ?artist-name
    :where
    [?a :artist/name   ?artist-name]
    [?t :track/artists ?a]
    [?t :track/name    ?title]
    [?m :medium/tracks ?t]
    [?r :release/media ?m]
    [?r :release/name  ?album]
    [?r :release/year  ?year]]
  db "John Lennon"))


(explain/q-explain
  '[:find ?title ?album ?year
    :in $ ?artist-name
    :where
    [?a :artist/name   ?artist-name]
    [?t :track/artists ?a]
    [?t :track/name    ?title]
    [?m :medium/tracks ?t]
    [?r :release/media ?m]
    [?r :release/name  ?album]
    [?r :release/year  ?year]]
  db "John Lennon")


(count
 (d/q
  '[:find ?title ?album ?year
    :in $ ?artist-name
    :where
    [?a :artist/name   ?artist-name]
    [?t :track/artists ?a]
    [?t :track/name    ?title]
    [?m :medium/tracks ?t]
    [?r :release/media ?m]
    [?r :release/name  ?album]
    [?r :release/year  ?year]
    [(<= ?year 1980)]]
  db "John Lennon"))


(explain/q-explain
  '[:find ?title ?album ?year
    :in $ ?artist-name
    :where
    [?a :artist/name   ?artist-name]
    [?t :track/artists ?a]
    [?t :track/name    ?title]
    [?m :medium/tracks ?t]
    [?r :release/media ?m]
    [?r :release/name  ?album]
    [?r :release/year  ?year]
    [(<= ?year 1980)]]
  db "John Lennon")


(def simple-rules
  '[;; Given ?t bound to track entity-ids, binds ?r to the corresponding
    ;; set of album release entity-ids
    [(track-release ?t ?r)
     [?m :medium/tracks ?t]
     [?r :release/media ?m]]

    ;; Supply track entity-ids as ?t, and the other parameters will be
    ;; bound to the corresponding information about the tracks
    [(track-info ?t ?track-name ?artist-name ?album ?year)
     [?t :track/name    ?track-name]
     [?t :track/artists ?a]
     [?a :artist/name   ?artist-name]
     (track-release ?t ?r)
     [?r :release/name  ?album]
     [?r :release/year  ?year]]])


(count
 (d/q
  '[:find ?title ?album ?year
    :in $ % ?artist-name
    :where
    [?a :artist/name   ?artist-name]
    [?t :track/artists ?a]
    [?t :track/name    ?title]
    (track-release ?t ?r)
    [?r :release/name  ?album]
    [?r :release/year  ?year]]
  db simple-rules "John Lennon"))


(explain/q-explain
 '[:find ?title ?album ?year
   :in $ % ?artist-name
   :where
   [?a :artist/name   ?artist-name]
   [?t :track/artists ?a]
   [?t :track/name    ?title]
   (track-release ?t ?r)
   [?r :release/name  ?album]
   [?r :release/year  ?year]]
 db simple-rules "John Lennon")


(count
 (d/q
  '[:find ?artist ?rname ?type
    :in $ ?aname
    :where
    [?a :artist/name ?aname]
    [?ar :abstractRelease/artists ?a]
    [?ar :abstractRelease/name ?rname]
    [?ar :abstractRelease/artistCredit ?artist]
    [?ar :abstractRelease/type ?type-e]
    [?type-e :db/ident ?type]]
  db "The Beatles"))


(explain/q-explain
 '[:find ?artist ?rname ?type
   :in $ ?aname
   :where
   [?a :artist/name ?aname]
   [?ar :abstractRelease/artists ?a]
   [?ar :abstractRelease/name ?rname]
   [?ar :abstractRelease/artistCredit ?artist]
   [?ar :abstractRelease/type ?type-e]
   [?type-e :db/ident ?type]]
 db "The Beatles")


(def collab-rules
  '[;; Generic transitive network walking, used by collaboration network
    ;; rule below

    ;; Supply:
    ;; ?e1 -- an entity-id
    ;; ?attr -- an attribute ident
    ;; and ?e2 will be bound to entity-ids such that ?e1 and ?e2 are both
    ;; values of the given attribute for some entity (?x)
    [(transitive-net-1 ?attr ?e1 ?e2)
     [?x ?attr ?e1]
     [?x ?attr ?e2]
     [(not= ?e1 ?e2)]]

    ;; Same as transitive-net-1, but search one more level of depth.  We
    ;; define this rule twice, once for each case, and the rule
    ;; represents the union of the two cases:
    ;; - The entities are directly related via the attribute
    ;; - The entities are related to the given depth (in this case 2) via the attribute
    [(transitive-net-2 ?attr ?e1 ?e2)
     (transitive-net-1 ?attr ?e1 ?e2)]
    [(transitive-net-2 ?attr ?e1 ?e2)
     (transitive-net-1 ?attr ?e1 ?x)
     (transitive-net-1 ?attr ?x ?e2)
     [(not= ?e1 ?e2)]]

    ;; Artist collaboration graph-walking rules, based on generic
    ;; graph-walk rule above

    ;; Supply an artist name as ?artist-name-1, an ?artist-name-2 will be
    ;; bound to the names of artists who directly collaborated with the
    ;; artist(s) having that name
    [(collab ?artist-name-1 ?artist-name-2)
     [?a1 :artist/name ?artist-name-1]
     (transitive-net-1 :track/artists ?a1 ?a2)
     [?a2 :artist/name ?artist-name-2]]

    ;; Alias for collab
    [(collab-net-1 ?artist-name-1 ?artist-name-2)
     (collab ?artist-name-1 ?artist-name-2)]

    ;; Collaboration network walk to depth 2
    [(collab-net-2 ?artist-name-1 ?artist-name-2)
     [?a1 :artist/name ?artist-name-1]
     (transitive-net-2 :track/artists ?a1 ?a2)
     [?a2 :artist/name ?artist-name-2]]])


(count
 (d/q
  '[:find ?aname2
    :in $ % ?aname
    :where (collab-net-2 ?aname ?aname2)]
  db collab-rules "Paul McCartney"))


(explain/q-explain
 '[:find ?aname2
   :in $ % ?aname
   :where (collab-net-2 ?aname ?aname2)]
 db collab-rules "Paul McCartney")


(d/q
 '[:find ?aname (count ?e)
   :with ?a
   :in $ ?criterion [?aname ...]
   :where
   [?a :artist/name ?aname]
   [?e ?criterion ?a]]
 db :abstractRelease/artists ["Jay-Z" "Beyoncé Knowles"])


(explain/q-explain
 '[:find ?aname (count ?e)
   :with ?a
   :in $ ?criterion [?aname ...]
   :where
   [?a :artist/name ?aname]
   [?e ?criterion ?a]]
 db :abstractRelease/artists ["Jay-Z" "Beyoncé Knowles"])


(count
 (d/q
  '[:find ?aname ?tname
    :in $ ?artist-name
    :where
    [?a :artist/name ?artist-name]
    [?t :track/artists ?a]
    [?t :track/name ?tname]
    [(not= "Outro" ?tname)]
    [(not= "[outro]" ?tname)]
    [(not= "Intro" ?tname)]
    [(not= "[intro]" ?tname)]
    [?t2 :track/name ?tname]
    [?t2 :track/artists ?a2]
    [(not= ?a2 ?a)]
    [?a2 :artist/name ?aname]]
  db "The Who"))

(explain/q-explain
 '[:find ?aname ?tname
   :in $ ?artist-name
   :where
   [?a :artist/name ?artist-name]
   [?t :track/artists ?a]
   [?t :track/name ?tname]
   [(not= "Outro" ?tname)]
   [(not= "[outro]" ?tname)]
   [(not= "Intro" ?tname)]
   [(not= "[intro]" ?tname)]
   [?t2 :track/name ?tname]
   [?t2 :track/artists ?a2]
   [(not= ?a2 ?a)]
   [?a2 :artist/name ?aname]]
 db "The Who")
