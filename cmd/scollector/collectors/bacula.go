package collectors

import (
	 _ "github.com/go-sql-driver/mysql"
	"database/sql"

	"bosun.org/metadata"
	"bosun.org/opentsdb"
	"bosun.org/slog"
)
	

func init() {
	collectors = append(collectors, &IntervalCollector{F: c_bacula_status})
}

func c_bacula_status() (opentsdb.MultiDataPoint, error) {
	slog.Error("WTF")
	db, err := sql.Open("mysql", "user:pass@/db")
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
