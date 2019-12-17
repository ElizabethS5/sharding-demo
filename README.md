# An Intro to Database Sharding

The `controller.py` file in this repository takes the file named `chapter2.txt` and emulates a simple horizontally sharded database, allowing easy understanding of how data is distributed across nodes. This particular handler is unfinished; it can initialize nodes and add nodes to an initialized list, but it cannot remove nodes or handle replication. That is an exercise for the reader.

Content in `chapter2.txt` retrieved from <https://www.bookbrowse.com/excerpts/index.cfm/book_number/452/page_number/1/harry-potter-and-the-sorcerers-stone#excerpt.>

To reset everything for a fresh run, just execute `make clean` in your terminal.

## In terminal to get into python shell

`python`

## In the python shell

`>>> import controller as c`

Note: If data folder is not already present, a data folder containing 5 text files will be created, then a 6th text file will be created in data, automatically. A mapping.json file will also be created in the parent directory. If this does not work, it may be because mapping.json already exists but data folder does not or data folder exists but mapping.json does not. In that case delete the existing mapping.json file or data folder and try again.

`>>> s = c.ShardHandler()`

## To add a shard (Horizontal Sharding)

`>>> s.add_shard()`

## To remove s shard

`>>> s.remove()`

## To add replication (Vertical Sharding)

`>>> s.add_replication()`

## To remove a last made level of replication

`>>> s.remove_replication()`

Note: If no relicated files remain attempting to remove replication will raise an error

## To replace missing primary and replicated files and sync replicated file mapping to primary files mapping

`>>> s.sync_replication()`

Note: If all copies of a file have been deleted, An error will be raised that files cannot be recovered.

## To get a word at a particular index

`>>> s.get_word_at_index(index)`

Note: Returns and prints a tuple including the filename of the file that the index was found in and the word at the index. If on a word of punctuation, it will find the following word (except the very last word of the last file while returns the last word). If a word is split between files it will still return the whole word but still only include the file from which the search was started.
