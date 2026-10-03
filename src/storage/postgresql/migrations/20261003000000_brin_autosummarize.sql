-- Summarize the BRIN ranges of the value tables as soon as they are complete, instead of waiting for
-- the next vacuum of the table.
--
-- A BRIN index only knows the ranges it has summarized; every range written since is returned in
-- full by every scan that uses the index. Without this a time-window read, and the probe of the
-- deduplication at ingestion, read all the rows written since the last (auto)vacuum of the table.
-- The cost is a little work for the autovacuum workers, no change for the writers.
ALTER INDEX index_integer_values SET (autosummarize = on);
ALTER INDEX index_numeric_values SET (autosummarize = on);
ALTER INDEX index_float_values SET (autosummarize = on);
ALTER INDEX index_string_values SET (autosummarize = on);
ALTER INDEX index_boolean_values SET (autosummarize = on);
ALTER INDEX index_location_values SET (autosummarize = on);
ALTER INDEX index_json_values SET (autosummarize = on);
ALTER INDEX index_blob_values SET (autosummarize = on);
