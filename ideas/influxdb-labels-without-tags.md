# InfluxDB importer: org and bucket are only added to lines that have tags

`src/http/influxdb.rs` adds `influxdb_org` and `influxdb_bucket` to the labels of a line, on purpose: they are
part of the identity of a series (the same measurement written to two buckets is two series). But it does so only
`Some(tags)`: a line without tags (`power_w value=5`) gets no labels at all, so the same measurement without tags
written to two buckets or two orgs is one series.

Probably an oversight rather than a decision. Either add the two labels to every line (a breaking change for
the ids of the series that already exist, which is fine before production), or write it down as intended.
