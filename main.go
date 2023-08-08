// Poll every store every hour and sotre the status in store status.csv

package main

import (
    "fmt"
    // "encoding/csv"
    "log"
    "database/sql"
    "github.com/go-sql-driver/mysql"
)

var db *sql.DB

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
}
