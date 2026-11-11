# Arrow import: the size check and the reader can disagree on the format

Source: review of pull request 60 on 11 November 2026 (HEAD `7d8b396`). Confirmed by reading the code and
`arrow-ipc` 60.1.0; not executed. **Blocker for the merge.**

## Problem

`check_decompressed_size` (`src/importers/arrow.rs:104-145`) picks the format from the first bytes:
`buffer.starts_with(b"ARROW1")` is a file, anything else is a stream.

`parse_arrow_file` (`:64-93`) picks it another way: it tries `StreamReader::try_new(..).ok()` and, on any
error, falls back to `FileReader::try_new`. `FileReader` only checks the trailing magic
(`arrow-ipc` `reader.rs:1289-1295`, `read_footer_length`), never the leading one.

Input: a valid LZ4-compressed Arrow file whose first four bytes are overwritten with zeros, and a buffer's
8-byte length prefix forged to a huge value.

- The check takes the stream branch, reads a message length of 0, returns "end of stream" with a total of 0,
  and passes.
- `StreamReader::try_new` fails, the error is discarded, and `FileReader` decodes the footer blocks unchecked.
- `arrow-ipc` allocates the declared size (`compression.rs:312`): an allocation failure aborts the process
  (not caught by the panic layer), or an LZ4 bomb with a plausible size.

The test `test_compressed_bodies_are_limited_by_their_declared_size` only forges a stream.

## Suggested solution

One decision for both: choose the reader by the same test as the check. A body that starts with `ARROW1`
goes to `FileReader` only; anything else goes to `StreamReader` only, and its error is returned as a 400.
Remove the fallback.

Also make the stream branch of the check strict: an empty stream (first length 0 or fewer than 4 bytes) is
"no data batches", a 400, before any reader runs.

## Tests

- The forged file above (leading magic zeroed, huge declared length): 400, and the process survives.
- A file with intact magic and a forged length: 400.
- A stream that the stream reader refuses: 400 with the stream reader's message.
- The Python SDK's LZ4 stream and a pyarrow file still import.

## Done when

The tests pass and no input reaches a reader that the size check did not walk.

## Outcome (11 November 2026)

The finding was right. The new test `test_a_file_with_a_damaged_leading_magic_is_refused` aborted the test
process (`memory allocation of 4611686018427387904 bytes failed`, SIGABRT) before the change.

- `parse_arrow_file` picks the reader with `is_file` (leading `ARROW1`), the same test as the size check: a
  file goes to `FileReader` only, anything else to `StreamReader` only, and the reader's error is returned.
  The fallback is gone.
- The stream branch of the check refuses a body with no first message (empty, fewer than 4 bytes, or a length
  of 0) with "contains no data batches", before any reader runs.
- Tests: forged file with intact and with zeroed magic, empty and truncated streams, and LZ4 streams and files
  of another writer still import. The Python SDK itself was not run; its LZ4 stream is what
  `compressed_zeros(_, false)` writes.
