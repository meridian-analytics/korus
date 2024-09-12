Change log
==========

**Version 0.0.9** (Month Day, Year)

 * 

**Version 0.0.8** (Sep 12, 2024)

 * Various improvements to `korus-submit`
 * `db.get_annotations` returns ambiguous labels
 * New function for printing annotation summary
 * Improved error handling when adding annotations
 * Fixed bug in parsing of ambiguous labels
 * Added column `valid` to annotation table
 * Added support for inserting annotations with invalid/missing file IDs into the database
 * Added `issues` column to `job` table and renamed `model_config` to `configuration`

**Version 0.0.7** (July 05, 2024)

 * Added support for filtering on ambiguous labels
 * Added functionality for extracting audio file metadata from within tar archives
 * Added functionality for searching for files within tar archives
 * For annotation jobs, comments can be parsed from plain text files instead of having to be typed in the console

**Version 0.0.6** (June 04, 2024)

 * Path autocompletion in `korus-submit`
 * Improved timestamp parsing in `korus-submit`
 * `granularity_id` replaces `granularity` column in `annotation` table
 * New `granularity` table

**Version 0.0.5** (May 21, 2024)

 * Improved error handling in `db.add_annotations`
 * Minor bug fix in `app.app_util.add.add_files` related to int/np.int64 conversion

**Version 0.0.4** (April 15, 2024)

 * Minor bug fix in `db.get_annotation` related to int/float conversion
 * Improved error handling in `db.insert_job`
 * Added `tree.TTree.is_ancestor` method (overwrites treelib's `is_ancestor` method)

**Version 0.0.3** (January 31, 2024)

 * Faster search for audio files when organized into date-stamped folders
 * Various improvements to `korus-submit` CLI tool
 * Added new tables `tag` and `storage`

**Version 0.0.2** (November 27, 2023)

 * New `selection` module
 * Added option to output annotation tables in future v3 Ketos format
 * Added option to overwrite existing entries when saving taxonomy
 * Implemented @avoid and @negative args in `db.filter_annotation`
 * Improved interface of `db.get_annotations` function
 * Extended `db.add_annotations` to be able to handle ambiguous label assignments
 * Created command-line tool `korus-submit` for adding new data to a database
 * Added function `filter_files` to search for audio files in the database
 * Refactored `db` module
 * Added `earliest_start_utc`, `latest_start_utc`, `rel_path` args to `db.util.collect_audiofile_metadata`
 * Implemented `tentative=True` in `db.filter_annotation`
 * A few minor bug fixes in `korus.db`

**Version 0.0.1** (August 2, 2023)

 * First release

