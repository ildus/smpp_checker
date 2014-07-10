package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	kannelConf   = flag.String("conf", "/etc/kannel/kannel.conf", "Location of kannel configuration")
	workersCount = flag.Int("workers", 10, "Workers count")
	sqlLimit     = flag.Int("limit", 10, "SQL query limit")
	conf         *map[string]*Connection
	db           *sql.DB
)

const (
	sqlRecords = "select id, ts, smsc, url, destination from dlr where status='0' order by id desc limit $1"
	sqlUpdate  = "update dlr set status=$1 where id=$2"
	statusUrl  = "http://smsc.ru/sys/status.php?login=%s&psw=%s&phone=%s&id=%d&fmt=3"
)

type Msg struct {
	id         int
	externalId int
	smsc       string
	url        string
	phone      string
}

type Connection struct {
	login    string
	psw      string
	host     string
	port     string
	database string //for pg configuration
}

type HttpResult struct {
	Status    int    `json:"status"`
	Err       int    `json:"err"`
	Error     string `json:"error"`
	ErrorCode int    `json:"error_code"`
}

func loadConfiguration() *map[string]*Connection {
	conf := make(map[string]*Connection)
	bin, err := ioutil.ReadFile(*kannelConf)
	if err != nil {
		log.Fatal(err)
	}
	var currentGroup string
	contents := strings.Split(string(bin), "\n")
	for _, line := range contents {
		parts := strings.Split(line, "=")
		for i, part := range parts {
			parts[i] = strings.Trim(part, " ")
		}
		if len(parts) != 2 {
			continue
		}
		key, val := parts[0], parts[1]
		if key == "group" {
			if val == "smsc" {
				currentGroup = "current"
				conf[currentGroup] = &Connection{}
			} else {
				if val == "pgsql-connection" {
					currentGroup = "pg"
					conf[currentGroup] = &Connection{
						port:     "5432",
						host:     "localhost",
						database: "dlr",
					}
				} else {
					currentGroup = ""
				}
			}
		} else {
			if len(currentGroup) > 0 {
				if key == "smsc-id" {
					conf[val] = conf["current"]
					currentGroup = val
					delete(conf, "current")
				}
				if key == "host" {
					conf[currentGroup].host = val
				}
				if key == "port" {
					conf[currentGroup].port = val
				}
				if key == "smsc-username" || key == "username" {
					conf[currentGroup].login = val
				}
				if key == "smsc-password" || key == "password" {
					conf[currentGroup].psw = val
				}
				if key == "database" {
					conf[currentGroup].database = val
				}
			}
		}

	}
	return &conf
}

func processMessage(msg *Msg) {
	if conn, exists := (*conf)[msg.smsc]; exists == true {
		login := url.QueryEscape(conn.login)
		psw := url.QueryEscape(conn.psw)
		phone := url.QueryEscape(msg.phone)

		url := fmt.Sprintf(statusUrl, login, psw, phone, msg.externalId)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Msg %d processing error: %v", msg.externalId, err)
			return
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Msg %d body read error: %v", msg.externalId, err)
			return
		}
		var httpResult HttpResult
		json.Unmarshal(body, &httpResult)

		if httpResult.ErrorCode > 0 {
			log.Printf("Msg %d http error: %s", msg.externalId, httpResult.Error)
		} else {
			if httpResult.Status > 0 {
				callbackUrl := fmt.Sprintf(msg.url, httpResult.Status)
				client := &http.Client{}
				req, _ := http.NewRequest("GET", callbackUrl, nil)
				req.Header.Add("SMSC-ERROR", string(httpResult.Err))
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("Msg %d callback error: %v", msg.externalId, err)
					return
				}
				body, err = ioutil.ReadAll(resp.Body)
				if err != nil {
					return
				}
				db.Exec(sqlUpdate, string(httpResult.Status), msg.id)
			}
		}
	}
}

func checkMessages(chm chan *Msg, wg *sync.WaitGroup) {
	defer wg.Done()
	for msg := range chm {
		processMessage(msg)
	}
}

func processRecords() {
	var wg sync.WaitGroup
	chm := make(chan *Msg, *workersCount)

	rows, err := db.Query(sqlRecords, *sqlLimit)
	defer rows.Close()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < *workersCount; i += 1 {
		wg.Add(1)
		go checkMessages(chm, &wg)
	}
	for rows.Next() {
		msg := Msg{}
		rows.Scan(&msg.id, &msg.externalId, &msg.smsc, &msg.url, &msg.phone)
		chm <- &msg
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	close(chm)
	wg.Wait()
}

func main() {
	flag.Parse()
	conf = loadConfiguration()

	go func() {
		sigchan := make(chan os.Signal, 10)
		signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)
		<-sigchan
		log.Println("Got interrupt signal. Exited")
		os.Exit(0)
	}()

	defer func() {
		if err := recover(); err != nil {
			log.Fatal("Program is exiting with exception: ", err)
		}
	}()

	pgConf := (*conf)["pg"]
	conn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", pgConf.login,
		pgConf.psw, pgConf.host, pgConf.port, pgConf.database)

	var err error
	db, err = sql.Open("postgres", conn)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	for {
		processRecords()
		time.Sleep(time.Minute * 5)
	}
}
