// Poll every store every hour and sotre the status in store status.csv

package main

import (
    "fmt"
    "encoding/csv"
    "log"
    "database/sql"
    "github.com/go-sql-driver/mysql"
    "github.com/google/uuid"
	"net/http"
    "os"
)

var db *sql.DB
var rows *sql.Rows

var m = make(map[string]int)

func main() {
    // Capture connection properties.
    cfg := mysql.Config{
        User:   "ishan",
        Passwd: "password",
        Net:    "tcp",
        Addr:   "127.0.0.1:3306",
        DBName: "store_monitor",
        AllowNativePasswords: true,
    }
    // Get a database handle.
    var err error
    db, err = sql.Open("mysql", cfg.FormatDSN())
    if err != nil {
        log.Fatal(err)
    }

    pingErr := db.Ping()
    if pingErr != nil {
        log.Fatal(pingErr)
    }
    fmt.Println("Connected!")

	defer db.Close()

    PORT := ":8080"

    http.HandleFunc("/trigger_report", TriggerReport)
    http.HandleFunc("/get_report", GetReport)

    log.Fatal(http.ListenAndServe(PORT, nil))
}

func TriggerReport(w http.ResponseWriter, r *http.Request) {
    if r.Method == "GET" {
        DropTempTables()
        CreateActiveTempTable()
        LoadActiveTempTable()
        fmt.Println("active temp created")
        CreateInactiveTempTable()
        LoadInactiveTempTable()
        fmt.Println("inactive temp created")

        u, _ := uuid.NewRandom()
        uuid := u.String()

        fmt.Println("uuid", uuid)

        m[uuid] = 0

        query := `
SELECT
            a.store_id,
            a.uptime_last_week,
            i.downtime_last_week
        FROM
            (SELECT
                 store_id,
                 SEC_TO_TIME(SUM(TIME_TO_SEC(uptime))) AS uptime_last_week
             FROM temp_active_store
             GROUP BY store_id) a
        JOIN
            (SELECT
                 store_id,
                 SEC_TO_TIME(SUM(TIME_TO_SEC(uptime))) AS downtime_last_week
             FROM temp_inactive_store
             GROUP BY store_id) i
        ON a.store_id = i.store_id;
    `
    rows, _ = db.Query(query)

    m[uuid] = 1
    }

    fmt.Println("ran the query")
}

func DropTempTables() error {
    stmt, err := db.Prepare(
        `
        drop temporary table if exists temp_active_store;
        `)

        if err != nil {
            panic(err.Error())
        }

        _, err = stmt.Exec()

        if err != nil {
            panic(err.Error())
        }

    stmt, err = db.Prepare(
        `
        drop temporary table if exists temp_inactive_store;
        `)

        if err != nil {
            panic(err.Error())
        }

        _, err = stmt.Exec()

        if err != nil {
            panic(err.Error())
        }

        return err
}

func CreateActiveTempTable() error {
    stmt, err := db.Prepare(
        `
        create temporary table temp_active_store (
            store_id BIGINT NOT NULL,
            date            DATE,
            uptime          TIME
        );`)

        if err != nil {
            panic(err.Error())
        }

        _, err = stmt.Exec()

        if err != nil {
            panic(err.Error())
        }

        return err
}

func LoadActiveTempTable() error {
    stmt, err := db.Prepare(
        `
        insert into temp_active_store
        select
            st.store_id,
            date(ss.timestamp_utc) as date,
            timediff(
                max(time(ss.timestamp_utc)), min(time(ss.timestamp_utc))) as uptime
        from store_timezone st
        inner join store_status ss on st.store_id = ss.store_id
        inner join store_business_hours sbh on sbh.store_id = ss.store_id
        where 
            time(ss.timestamp_utc) between sbh.start_time_local and sbh.end_time_local
            and weekday(ss.timestamp_utc) = sbh.day_of_week
            and ss.status = "active"
        group by st.store_id, date(ss.timestamp_utc)
        order by st.store_id;
        `)

        if err != nil {
            panic(err.Error())
        }

        _, err = stmt.Exec()

        if err != nil {
            panic(err.Error())
        }

        return err
}

func CreateInactiveTempTable() error {
    stmt, err := db.Prepare(
        `
        create temporary table temp_inactive_store (
            store_id BIGINT NOT NULL,
            date            DATE,
            uptime          TIME
        );`)

        if err != nil {
            panic(err.Error())
        }

        _, err = stmt.Exec()

        if err != nil {
            panic(err.Error())
        }

        return err
}

func LoadInactiveTempTable() error {
    stmt, err := db.Prepare(
        `
        insert into temp_inactive_store
        select
            st.store_id,
            date(ss.timestamp_utc) as date,
            timediff(
                max(time(ss.timestamp_utc)), min(time(ss.timestamp_utc))) as downtime
        from store_timezone st
        inner join store_status ss on st.store_id = ss.store_id
        inner join store_business_hours sbh on sbh.store_id = ss.store_id
        where 
            time(ss.timestamp_utc) between sbh.start_time_local and sbh.end_time_local
            and weekday(ss.timestamp_utc) = sbh.day_of_week
            and ss.status = "inactive"
        group by st.store_id, date(ss.timestamp_utc)
        order by st.store_id;
        `)

        if err != nil {
            panic(err.Error())
        }

        _, err = stmt.Exec()

        if err != nil {
            panic(err.Error())
        }

        return err
}

func GetReport(w http.ResponseWriter, r *http.Request) {
    id := r.URL.Query().Get("id")
    fmt.Println(id)
    defer rows.Close()
    if (m[id] == 1) {
        fmt.Println("starting to write to csv")
        csvFile, err := os.Create("report.csv")
        if err != nil {
            log.Fatal(err)
        }
        defer csvFile.Close()

        csvWriter := csv.NewWriter(csvFile)
        defer csvWriter.Flush()

        // Write CSV header
        csvWriter.Write([]string{"store_id", "uptime_last_week", "downtime_last_week"})

        // Iterate through rows and write to CSV
        fmt.Println("Iteration through rows")
        for rows.Next() {
            var storeID string
            var uptimeLastWeek string
            var downtimeLastWeek string
            if err := rows.Scan(&storeID, &uptimeLastWeek, &downtimeLastWeek); err != nil {
                log.Fatal(err)
            }
            csvWriter.Write([]string{storeID, uptimeLastWeek, downtimeLastWeek})
        }
        fmt.Println("Iteration through rows complete")
    }
}
