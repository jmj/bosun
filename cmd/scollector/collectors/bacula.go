package collectors

import (
	"time"
	"fmt"

	 _ "github.com/go-sql-driver/mysql"
	"database/sql"

	"bosun.org/metadata"
	"bosun.org/opentsdb"
	"bosun.org/slog"
)

func Bacula(user, pass, dbase string) error {
	collectors = append(collectors, &IntervalCollector{
		F: func() (opentsdb.MultiDataPoint, error) {
			return c_bacula_status(user, pass, dbase)
		},
		Enable:		func() bool {
			return baculaEnable(user, pass, dbase)
		},
		Interval:	2 * time.Hour,
		name:		"bacula",
	})

	return nil
}

func baculaEnable(user, pass, dbase string) bool {
	dsn := fmt.Sprintf("%s:%s@/%s", user, pass, dbase)
	db, err := sql.Open("mysql", dsn)
	defer db.Close()
	return err == nil
}

func c_bacula_status(user, pass, dbase string) (opentsdb.MultiDataPoint, error) {
	dsn := fmt.Sprintf("%s:%s@/%s", user, pass, dbase)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		slog.Error("Failed to connect to database")
		return nil, err
	}
	defer db.Close()

	var md opentsdb.MultiDataPoint

	var name string
	var value int
	var tagSet opentsdb.TagSet
	var rate metadata.RateType
	var unit metadata.Unit
	var description string

	name = "linux.bacula"
	value = 10
	tagSet = nil
	rate = metadata.Unknown
	unit = metadata.None
	description = "this is a desc"

	rows, err := db.Query("SELECT DISTINCT(Name) from Job")
	if err != nil {
		slog.Error("Query Error: " + err.Error())
		return nil, err
	}

	for rows.Next() {
		rows.Scan(&name)

		r := db.QueryRow("SELECT count(JobId) as value from Job where RealEndTime>SUBTIME(now(), '7 0:0:0') and JobStatus='T' and Name=?", name)
		
		r.Scan(&value)

		slog.Infoln(name, value)

		Add(&md, "bacula."+name, value, tagSet, rate, unit, description)
	}
	
	return md, nil
}
