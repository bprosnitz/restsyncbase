package main

import (
	"fmt"

	"flag"
	"net/http"

	"strings"

	"log"

	"io/ioutil"

	"encoding/json"

	"v.io/v23"
	"v.io/v23/context"
	"v.io/v23/syncbase"
	"v.io/v23/syncbase/nosql"
	_ "v.io/x/ref/runtime/factories/generic"
)

var addr = flag.String("addr", ":8080", "address to serve on")
var serviceName = flag.String("service", "", "name of syncbase service to connect to")
var appName = flag.String("app", "", "name of app to connect to")

var app syncbase.App
var ctx *context.T

func main() {
	var shutdown func()
	ctx, shutdown = v23.Init()
	defer shutdown()

	// TODO(bprosnitz) Do we need to worry about a race between Exists() and Create()?
	app = syncbase.NewService(*serviceName).App(*appName)
	exists, err := app.Exists(ctx)
	if err != nil {
		log.Fatalf("error in app.Exists(): %v", exists)
	}
	if !exists {
		if err := app.Create(ctx, nil); err != nil {
			log.Fatalf("error in app.Create(): %v", err)
		}
	}

	http.HandleFunc("/", handler)
	http.ListenAndServe(*addr, nil)
}

func handler(w http.ResponseWriter, r *http.Request) {
	strs := strings.SplitN(strings.TrimLeft(r.URL.Path, "/"), "/", 3)

	if len(strs) == 0 {
		http.Error(w, "expected request of the form /[Database Name]/[Action Name]/...", 400)
		return
	}

	databaseName := strs[0]
	db := app.NoSQLDatabase(databaseName, nil)
	method := r.Method

	if len(strs) == 1 {
		handleDatabase(w, db, method)
		return
	}

	action := strings.ToLower(strs[1])

	var remainder string
	if len(strs) > 2 {
		remainder = strs[2]
	}

	switch {
	case action == "rest":
		handleRest(w, db, remainder, r)
	case action == "query":
		handleQuery(w, db, remainder, r)
	case action == "syncgroup":
		handleSyncgroup(w, db, remainder, r)
	default:
		http.Error(w, "unknown action", 400)
	}
}

func handleRest(w http.ResponseWriter, db nosql.Database, remainder string, r *http.Request) {
	strs := strings.SplitN(remainder, "/", 2)

	method := r.Method

	exists, err := db.Exists(ctx)
	if err != nil {
		http.Error(w, "error in db.Exists()", 500)
		return
	}
	if !exists {
		http.Error(w, "database doesn't exist", 404)
		return
	}
	tableName := strs[0]

	t := db.Table(tableName)
	if len(strs) == 1 {
		handleTable(w, db, t, method)
		return
	}

	// TODO(bprosnitz) Is there a race between table Exists and Create?
	exists, err = t.Exists(ctx)
	if err != nil {
		http.Error(w, "error in table.Exists()", 500)
		return
	}
	if !exists {
		http.Error(w, "table doesn't exist", 404)
		return
	}

	key := strs[1]
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error reading message body", 500)
		return
	}

	fmt.Printf("DB: %s Table: %s Key: %s method: %s value: %q\n", db.Name(), tableName, key, method, body)
	row := t.Row(key)
	handleRow(w, row, key, string(body), method)
}

func handleDatabase(w http.ResponseWriter, db nosql.Database, method string) {
	switch {
	case method == "GET":
		exists, err := db.Exists(ctx)
		if err != nil {
			http.Error(w, "error in db.Exists()", 500)
			return
		}
		if !exists {
			http.Error(w, "db doesn't exist", 404)
			return
		}

		tables, err := db.ListTables(ctx)
		if err != nil {
			http.Error(w, "error in db.ListTables()", 500)
			return
		}
		urls := make([]string, len(tables))
		for i, tableName := range tables {
			urls[i] = fmt.Sprintf("//%s/%s", db.Name(), tableName)
		}
		jsondat, err := json.Marshal(urls)
		if err != nil {
			http.Error(w, "error generating JSON", 500)
			return
		}
		w.Header().Add("Content-type", "Application/json")
		w.Write(jsondat)
	case method == "POST":
		exists, err := db.Exists(ctx)
		if err != nil {
			http.Error(w, "error in db.Exists()", 500)
			return
		}
		if exists {
			http.Error(w, "database already exists", 409)
			return
		}
		if err := db.Create(ctx, nil); err != nil {
			http.Error(w, "error creating database", 500)
			return
		}
	default:
		http.Error(w, "unsupported method for database", 400)
		return
	}
}

func handleTable(w http.ResponseWriter, db nosql.Database, t nosql.Table, method string) {
	switch {
	case method == "GET":
		exists, err := t.Exists(ctx)
		if err != nil {
			http.Error(w, "error in table.Exists()", 500)
			return
		}
		if !exists {
			http.Error(w, "table doesn't exist", 404)
			return
		}

		rnge := nosql.Prefix("")
		stream := t.Scan(ctx, rnge)
		urls := []string{}
		for stream.Advance() {
			url := fmt.Sprintf("//%s/%s/%s", db.Name(), t.Name(), stream.Key())
			urls = append(urls, url)
		}
		jsondat, err := json.Marshal(urls)
		if err != nil {
			http.Error(w, "error generating JSON", 500)
			return
		}
		w.Header().Add("Content-type", "Application/json")
		w.Write(jsondat)
	case method == "POST":
		exists, err := t.Exists(ctx)
		if err != nil {
			http.Error(w, "error in table.Exists()", 500)
			return
		}
		if exists {
			http.Error(w, "table already exists", 409)
			return
		}
		if err := t.Create(ctx, nil); err != nil {
			http.Error(w, "error creating table", 500)
			return
		}
	default:
		http.Error(w, "unsupported method for database", 400)
		return

	}
}

func handleRow(w http.ResponseWriter, row nosql.Row, key string, body string, method string) {
	switch {
	case method == "GET", method == "PUT", method == "DELETE":
		exists, err := row.Exists(ctx)
		if err != nil {
			http.Error(w, "error in row.Exists()", 500)
			return
		}
		if !exists {
			http.Error(w, "specified row does not exist", 404)
			return
		}
	case method == "POST":
		exists, err := row.Exists(ctx)
		if err != nil {
			http.Error(w, "error in row.Exists()", 500)
			return
		}
		if exists {
			http.Error(w, "specified row already exists", 409)
			return
		}
	}

	switch {
	case method == "GET":
		var str string // TODO(bprosnitz) Replace once JSON transcoding works
		if err := row.Get(ctx, &str); err != nil {
			http.Error(w, "error reading value from table", 500)
			return
		}
		if _, err := w.Write([]byte(str)); err != nil {
			http.Error(w, "error writing response", 500)
			return
		}
	case method == "POST", method == "PUT":
		if err := row.Put(ctx, body); err != nil {
			http.Error(w, "error putting to value", 500)
			return
		}
	case method == "DELETE":
		if err := row.Delete(ctx); err != nil {
			http.Error(w, "error deleting row", 500)
			return
		}
	}
}

func handleQuery(w http.ResponseWriter, db nosql.Database, query string, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "unsupported HTTP method", 400)
		return
	}
	headers, rs, err := db.Exec(ctx, query)
	if err != nil {
		http.Error(w, fmt.Sprintf("error in db.Exec(): %v", err), 500)
		return
	}
	fmt.Printf("Headers: %v\n", headers)
	results := []map[string]string{}
	for rs.Advance() {
		rowVals := rs.Result()
		result := map[string]string{}
		for i, elem := range rowVals {
			// TODO(bprosnitz) This assumes that we have string values
			result[headers[i]] = elem.String()
		}
		results = append(results, result)
	}
	jsondat, err := json.Marshal(results)
	if err != nil {
		http.Error(w, "error generating JSON", 500)
		return
	}
	w.Header().Add("Content-type", "Application/json")
	w.Write(jsondat)
}

func handleSyncgroup(w http.ResponseWriter, db nosql.Database, query string, r *http.Request) {
	http.Error(w, "syncbase support not yet implemented", 500)
}
