load data infile "d:/Programming/stuff/restraunts/csv/bq-results-20230125-202210-1674678181880.csv"
into table store_timezone
fields terminated by ','
enclosed by '"'
lines terminated by "\n"
ignore 1 rows;

load data infile "d:/Programming/stuff/restraunts/csv/store status.csv"
into table store_status
fields terminated by ','
enclosed by '"'
lines terminated by "\n"
ignore 1 rows;

load data infile "d:/Programming/stuff/restraunts/csv/Menu hours.csv"
into table store_business_hours
fields terminated by ','
enclosed by '"'
lines terminated by "\n"
ignore 1 rows;
