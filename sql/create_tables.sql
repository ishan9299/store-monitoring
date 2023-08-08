create database restaurants;

use restaurants;

drop table if exists store_timezone;
create table store_timezone (
	store_id 	 BIGINT NOT NULL,
    timezone_str VARCHAR(30),
    PRIMARY KEY (`store_id`)
);

drop table if exists store_status;
create table store_status (
	store_id       BIGINT NOT NULL,
	status         ENUM("active", "inactive") NOT NULL,
	timestamp_utc  TIMESTAMP NOT NULL,
	FOREIGN KEY (`store_id`) REFERENCES store_timezone(store_id)
);

drop table if exists store_business_hours;
create table store_business_hours (
	store_id         BIGINT NOT NULL,
    day_of_week      INT,
    start_time_local TIME,
    end_time_local   TIME,
    FOREIGN KEY (`store_id`) REFERENCES store_timezone(store_id)
);
