-- The texts of the dictionaries may be of any length. A B-tree entry holds at most about 2.7 kB,
-- so the unique constraint on a longer text refused it, and the whole batch with it. Each text is
-- now unique through its md5, which the upserts conflict on and the lookups use (with the text
-- itself, to be exact). The explicit B-tree indexes on the texts duplicated the unique constraints
-- and had the same limit: they go too.
--
-- No BEGIN or COMMIT here: sqlx runs each migration in a transaction of its own.

ALTER TABLE units DROP CONSTRAINT units_name_key;
DROP INDEX index_units_name;
CREATE UNIQUE INDEX units_name_md5 ON units (md5(name));

ALTER TABLE labels_name_dictionary DROP CONSTRAINT labels_name_dictionary_name_key;
DROP INDEX index_labels_name_dictionary_name;
CREATE UNIQUE INDEX labels_name_dictionary_name_md5 ON labels_name_dictionary (md5(name));

ALTER TABLE labels_description_dictionary
    DROP CONSTRAINT labels_description_dictionary_description_key;
DROP INDEX index_labels_description_dictionary_description;
CREATE UNIQUE INDEX labels_description_dictionary_description_md5
    ON labels_description_dictionary (md5(description));

ALTER TABLE strings_values_dictionary DROP CONSTRAINT strings_values_dictionary_value_key;
DROP INDEX index_strings_values_dictionary_value;
CREATE UNIQUE INDEX strings_values_dictionary_value_md5
    ON strings_values_dictionary (md5(value));
