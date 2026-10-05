# Frontend: do not spend a column on a label that never changes

The series list has one column per label dimension. Data written with the InfluxDB endpoint carries
`influxdb_org` and `influxdb_bucket` on every series (the importer adds them), so two of the columns, and two
chips of every metric row, say the same thing on every row.

Idea: when every series of the list has the same value for a dimension, say it once above the table
("same for all: influxdb_org=sensapp") instead of a column. Worth doing if people write through Influx
mostly; the other option is to ask whether the importer should add those labels at all.
