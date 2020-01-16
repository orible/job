package job

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var flagConfig = flag.String("config", "", "config file location")

type JobLog func(s string, params ...interface{})

func (t *JobState) jobLog(s string, params ...interface{}) {
	h := fmt.Sprintf("[job/log] %s => ", t.name)
	fmt.Printf(h+s+" \n", params...)
}

type JobFuncCtx struct {
	DB      *sqlx.DB
	Log     JobLog
	lastRun int
}

// JobFunc ...
type JobFunc func(f *JobFuncCtx) int

type JobState struct {
	parent       *Job
	name         string
	fn           JobFunc
	runs         int64
	stopped      bool
	everySeconds int
	ticker       *time.Ticker
	sigstop      chan int
}

type JobMessage struct {
	index int
}

func (t *JobState) funcHandler() {
	fmt.Printf("[job/start] init %s/%s {every %d s}\n", t.parent.name, t.name, t.everySeconds)
	t.fn(&JobFuncCtx{
		DB:  t.parent.db,
		Log: t.jobLog,
	})

	for {
		select {
		case <-t.sigstop:
			fmt.Printf("[job] stopping\n")
			return
		case e := <-t.ticker.C:
			fmt.Println("[job] tick starting at", e)
			t.fn(&JobFuncCtx{
				DB:  t.parent.db,
				Log: t.jobLog,
			})
			fmt.Println("[job] tick ended at", e)
		}
	}
}

// Job ...
type Job struct {
	name string
	db   *sqlx.DB
	jobs []JobState
}

// AddJob ...
func (t *Job) AddJob(name string, everySeconds int, f JobFunc) {
	t.jobs = append(t.jobs, JobState{
		parent:       t,
		name:         name,
		fn:           f,
		runs:         0,
		sigstop:      make(chan int),
		stopped:      false,
		everySeconds: everySeconds,
		ticker:       time.NewTicker(time.Duration(everySeconds) * time.Second),
	})
}

func (t *Job) StopAll() {
	for i, z := range t.jobs {
		fmt.Printf("[job/stop] signal to %d\n", i)
		z.sigstop <- 1
	}
}

// RunLock ...
func (t *Job) RunLock() {

	defer t.db.Close()

	fmt.Printf("[job] \"%s\" started\n", t.name)
	fmt.Printf("[job] (%d) jobs registered\n", len(t.jobs))

	if len(t.jobs) == 0 {
		fmt.Println("[job] no jobs in queue, stopping")
		return
	}

	// start all jobs
	for index := 0; index < len(t.jobs); index++ {
		go t.jobs[index].funcHandler()
	}

	// wait for external signal to stop
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	fmt.Println("[job] interrupt")
	t.StopAll()
	fmt.Println("[job] all stopped")
}

type ConfigDatabase struct {
	Schema      string
	Host        string
	Port        int
	Account     string
	Password    string
	SessionKeys [][]byte
}

func ParseConfig(filename string, in *ConfigDatabase) bool {
	file, _ := ioutil.ReadFile(filename)
	err := json.Unmarshal(file, &(*in))
	if err != nil {
		fmt.Printf("[file] read error: %v\n", err)
		return false
	}
	return true
}

// NewJob ...
// Open timed job on database
func NewJob(name string) *Job {
	flag.Parse()

	if *flagConfig == "" {

	}

	var config ConfigDatabase
	if !ParseConfig("./db.json", &config) {
		fmt.Println("[init] Failed to read or parse db.json")
		return nil
	}

	f, errLog := os.OpenFile(name+".log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if errLog != nil {
		log.Fatalf("error opening file: %v", errLog)
	}

	defer f.Close()
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)

	db, err := sqlx.Open(
		"mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci",
			config.Account, config.Password,
			config.Host, config.Port, config.Schema))
	if err != nil {
		log.Print("[init] Failed to connect to database", err)
		return nil
	}

	err = db.Ping()
	if err != nil {
		fmt.Println("[init] Failed to connect to database", err.Error())
		log.Println("[init] Failed to connect to database" + err.Error())
		return nil
	}

	return &Job{
		db:   db,
		name: name,
	}
}
