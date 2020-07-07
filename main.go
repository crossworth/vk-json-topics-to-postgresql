package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
	"sync"

	vk "github.com/crossworth/vk-topic-to-json"
	_ "github.com/lib/pq"
	"github.com/travelaudience/go-sx"
)

func main() {
	var folder string
	var postgreSQLDSN string // Example DSN: "postgres://postgres:root@localhost/database?sslmode=disable"
	var migrate bool
	var workers int

	flag.StringVar(&folder, "folder", "backup", "Folder of JSON files")
	flag.StringVar(&postgreSQLDSN, "postgresql", "", "PostgreSQL DSN")
	flag.BoolVar(&migrate, "migrate", false, "If the application should migrate the database")
	flag.IntVar(&workers, "workers", 10, "Number of workers to use when migrating posts")
	flag.Parse()

	if postgreSQLDSN == "" {
		log.Fatalln("you must provide the postgresql")
	}

	files, err := filepath.Glob(folder + "/*.json")
	if err != nil {
		log.Fatalf("could not read the JSON files, %v", err)
	}

	db, err := sql.Open("postgres", postgreSQLDSN)
	if err != nil {
		log.Fatalf("could not create postgresql client, %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("could not connect to postgresql, %v", err)
	}

	if migrate {
		log.Printf("migrating database schema")

		err = migrateDatabaseTables(db)
		if err != nil {
			log.Fatalf("error migrating database schema, %v", err)
		}
	}

	err = checkDatabaseTables(db)
	if err != nil {
		log.Fatalf("database schema error, %v", err)
	}

	fileChan := make(chan string)

	go func() {
		for _, file := range files {
			fileChan <- file
		}

		close(fileChan)
	}()

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			for file := range fileChan {
				processFile(db, file)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	log.Printf("done")
}

func processFile(db *sql.DB, file string) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		log.Printf("could not read the file %s, %v\n", file, err)
		return
	}

	var topic vk.Topic
	err = json.Unmarshal(content, &topic)
	if err != nil {
		log.Printf("could not decode the file %s, %v\n", file, err)
		return
	}

	topicFromDB, err := findTopic(db, topic.ID)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("error reading postgresql data, %v\n", err)
		return
	}

	if topicFromDB.ID != 0 && topic.UpdatedAt == topicFromDB.UpdatedAt {
		log.Printf("topic %d already updated\n", topic.ID)
		return
	}

	if topicFromDB.UpdatedAt > topic.UpdatedAt {
		log.Printf("topic %d is older than topic from database\n", topic.ID)
		return
	}

	err = saveOrUpdateTopic(db, topic)

	updating := false
	if topicFromDB.ID != 0 {
		updating = true
	}

	if updating {
		if err != nil {
			log.Printf("could not update topic %d, %v\n", topic.ID, err)
			return
		}

		log.Printf("topic %d updated\n", topic.ID)
	} else {
		if err != nil {
			log.Printf("could not create topic %d, %v\n", topic.ID, err)
			return
		}

		log.Printf("topic %d created\n", topic.ID)
	}
}

func findTopic(db *sql.DB, id int) (vk.Topic, error) {
	var topic vk.Topic

	err := sx.Do(db, func(tx *sx.Tx) {
		tx.MustQueryRow(`SELECT id, updated_at FROM topics WHERE id = $1`, id).MustScan(&topic.ID, &topic.UpdatedAt)
	})

	return topic, err
}

func saveOrUpdateTopic(db *sql.DB, topic vk.Topic) error {
	err := sx.Do(db, func(tx *sx.Tx) {
		profileQuery := `INSERT INTO profiles VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET first_name = $2, last_name = $3, screen_name = $4, photo = $5`

		// NOTE(Pedro): This fix an odd behaviour where the profile is not
		// listed
		topic.Profiles[topic.CreatedBy.ID] = topic.CreatedBy
		topic.Profiles[topic.UpdatedBy.ID] = topic.UpdatedBy

		for _, profile := range topic.Profiles {
			tx.MustExec(profileQuery, profile.ID, profile.FirstName, profile.LastName, profile.ScreenName, profile.Photo)
		}

		topicQuery := `INSERT INTO topics VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (id) DO UPDATE SET title = $2, is_closed = $3, is_fixed = $4, created_at = $5, updated_at = $6, created_by = $7, updated_by = $8`
		tx.MustExec(topicQuery, topic.ID, topic.Title, topic.IsClosed, topic.IsFixed, topic.CreatedAt, topic.UpdatedAt, topic.CreatedBy.ID, topic.UpdatedBy.ID, false)

		commentQuery := `INSERT INTO comments VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (id) DO UPDATE SET from_id = $2, date = $3, text = $4, likes = $5, reply_to_uid = $6, reply_to_cid = $7`
		attachmentQuery := `INSERT INTO attachments  VALUES($1, $2) ON CONFLICT (comment_id, content) DO UPDATE SET content = $1, comment_id = $2`

		for _, comment := range topic.Comments {
			tx.MustExec(commentQuery, comment.ID, comment.FromID, comment.Date, comment.Text, comment.Likes, comment.ReplyToUID, comment.ReplyToCID, topic.ID, comment.FromID)

			for _, attachment := range comment.Attachments {
				tx.MustExec(attachmentQuery, attachment, comment.ID)
			}
		}

		pollQuery := `INSERT INTO polls VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO UPDATE SET question = $2, votes = $3, multiple = $4, end_date = $5, closed = $6`
		tx.MustExec(pollQuery, topic.Poll.ID, topic.Poll.Question, topic.Poll.Votes, topic.Poll.Multiple, topic.Poll.EndDate, topic.Poll.Closed, topic.ID)

		pollAnswerQuery := `INSERT INTO poll_answers VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET text = $2, votes = $3, rate = $4`
		for _, answer := range topic.Poll.Answers {
			tx.MustExec(pollAnswerQuery, answer.ID, answer.Text, answer.Votes, answer.Votes, topic.Poll.ID)
		}
	})

	return err
}

var neededTables = []string{
	"topics",
	"comments",
	"profiles",
	"polls",
	"poll_answers",
	"attachments",
}

func checkDatabaseTables(db *sql.DB) error {
	tables, err := getTables(db)
	if err != nil {
		return err
	}

	for _, neededTable := range neededTables {
		found := false

		for _, table := range tables {
			if neededTable == table {
				found = true
			}
		}

		if !found {
			return fmt.Errorf("table %s missing", neededTable)
		}
	}

	return nil
}

func getTables(db *sql.DB) ([]string, error) {
	var tables []string

	err := sx.Do(db, func(tx *sx.Tx) {
		tx.MustQuery(`SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';`).Each(func(r *sx.Rows) {
			var table string
			r.MustScan(&table)
			tables = append(tables, table)
		})
	})

	return tables, err
}

func migrateDatabaseTables(db *sql.DB) error {
	err := sx.Do(db, func(tx *sx.Tx) {
		queries := strings.Split(schema, ";")

		for _, q := range queries {
			q = strings.TrimSpace(q)
			if len(q) > 0 {
				tx.MustExec(q)
			}
		}
	})
	return err
}

const schema = `
CREATE TABLE "profiles"
(
 "id"          int NOT NULL,
 "first_name"  varchar(250) NOT NULL,
 "last_name"   varchar(250) NOT NULL,
 "screen_name" varchar(250) NOT NULL,
 "photo"       varchar(250) NOT NULL,
 CONSTRAINT "PK_profiles" PRIMARY KEY ( "id" )
);

CREATE TABLE "topics"
(
 "id"            int NOT NULL,
 "title"         varchar(250) NOT NULL,
 "is_closed"     boolean NOT NULL,
 "is_fixed"      boolean NOT NULL,
 "created_at"    bigint NOT NULL,
 "updated_at"    bigint NOT NULL,
 "created_by"    int NOT NULL,
 "updated_by"    int NOT NULL,
 "deleted"       boolean NOT NULL,
 CONSTRAINT "PK_topics" PRIMARY KEY ( "id" ),
 CONSTRAINT "FK_34" FOREIGN KEY ( "created_by" ) REFERENCES "profiles" ( "id" ) ON DELETE CASCADE,
 CONSTRAINT "FK_37" FOREIGN KEY ( "updated_by" ) REFERENCES "profiles" ( "id" ) ON DELETE CASCADE
);

CREATE INDEX "fkIdx_34" ON "topics"
(
 "created_by"
);

CREATE INDEX "fkIdx_37" ON "topics"
(
 "updated_at"
);

CREATE TABLE "comments"
(
 "id"           int NOT NULL,
 "from_id"      int NOT NULL,
 "date"         bigint NOT NULL,
 "text"         text NOT NULL,
 "likes"        int NOT NULL,
 "reply_to_uid" int NOT NULL,
 "reply_to_cid" int NOT NULL,
 "topic_id"     int NOT NULL,
 "profile_id"   int NOT NULL,
 CONSTRAINT "PK_comments" PRIMARY KEY ( "id" ),
 CONSTRAINT "FK_69" FOREIGN KEY ( "topic_id" ) REFERENCES "topics" ( "id" ) ON DELETE CASCADE,
 CONSTRAINT "FK_72" FOREIGN KEY ( "profile_id" ) REFERENCES "profiles" ( "id" ) ON DELETE CASCADE
);

CREATE INDEX "fkIdx_69" ON "comments"
(
 "topic_id"
);

CREATE INDEX "fkIdx_72" ON "comments"
(
 "profile_id"
);

CREATE TABLE "attachments"
(
 "content"    text NOT NULL,
 "comment_id" int NOT NULL,
 PRIMARY KEY (comment_id, content),
 CONSTRAINT "FK_79" FOREIGN KEY ( "comment_id" ) REFERENCES "comments" ( "id" ) ON DELETE CASCADE
);

CREATE INDEX "fkIdx_79" ON "attachments"
(
 "comment_id"
);

CREATE TABLE "polls"
(
 "id"       int NOT NULL,
 "question" varchar(250) NOT NULL,
 "votes"    int NOT NULL,
 "multiple" boolean NOT NULL,
 "end_date" bigint NOT NULL,
 "closed"   boolean NOT NULL,
 "topic_id" int NOT NULL,
 CONSTRAINT "PK_poll" PRIMARY KEY ( "id" ),
 CONSTRAINT "FK_57" FOREIGN KEY ( "topic_id" ) REFERENCES "topics" ( "id" ) ON DELETE CASCADE
);

CREATE INDEX "fkIdx_57" ON "polls"
(
 "topic_id"
);

CREATE TABLE "poll_answers"
(
 "id"      int NOT NULL,
 "text"    varchar(250) NOT NULL,
 "votes"   int NOT NULL,
 "rate"    real NOT NULL,
 "poll_id" int NOT NULL,
 CONSTRAINT "PK_poll_answers" PRIMARY KEY ( "id" ),
 CONSTRAINT "FK_54" FOREIGN KEY ( "poll_id" ) REFERENCES "polls" ( "id" ) ON DELETE CASCADE
);

CREATE INDEX "fkIdx_54" ON "poll_answers"
(
 "poll_id"
);
`
