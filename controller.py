import json
import os
from shutil import copyfile
from typing import List, Dict

filename = "chapter2.txt"


def load_data_from_file(path=None) -> str:
    with open(path if path else filename, 'r') as f:
        data = f.read()
    return data  # noqa


class ShardHandler(object):
    """
    Take any text file and shard it into X number of files with
    Y number of replications.
    """

    def __init__(self):
        self.mapping = self.load_map()
        self.last_char_position = 0
        self.replication_level = self.find_highest_replication_level()

    def find_highest_replication_level(self):
        levels = set()
        for key in list(self.mapping):
            if '-' in key:
                levels.add(int(key.split('-')[1]))
        return max(levels) if levels else 0

    mapfile = "mapping.json"

    def write_map(self) -> None:
        """Write the current 'database' mapping to file."""
        with open(self.mapfile, 'w') as m:
            json.dump(self.mapping, m, indent=2)

    def load_map(self) -> Dict:
        """Load the 'database' mapping from file."""
        if not os.path.exists(self.mapfile):
            return dict()
        with open(self.mapfile, 'r') as m:
            return json.load(m)

    def _reset_char_position(self):
        self.last_char_position = 0

    def get_shard_ids(self):
        return sorted([key for key in self.mapping.keys() if '-' not in key])

    def get_replication_ids(self):
        return sorted([key for key in self.mapping.keys() if '-' in key])

    def build_shards(self, count: int, data: str = None) -> [str, None]:
        """Initialize our miniature databases from a clean mapfile. Cannot
        be called if there is an existing mapping -- must use add_shard() or
        remove_shard()."""
        if self.mapping != {}:
            return "Cannot build shard setup -- sharding already exists."

        spliced_data = self._generate_sharded_data(count, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

    def _write_shard_mapping(self, num: str, data: str, replication=False):
        """Write the requested data to the mapfile. The optional `replication`
        flag allows overriding the start and end information with the shard
        being replicated."""
        if replication:
            parent_shard = self.mapping.get(num[:num.index('-')])
            self.mapping.update(
                {
                    num: {
                        'start': parent_shard['start'],
                        'end': parent_shard['end']
                    }
                }
            )
        else:
            if int(num) == 0:
                # We reset it here in case we perform multiple write operations
                # within the same instantiation of the class. The char position
                # is used to power the index creation.
                self._reset_char_position()

            self.mapping.update(
                {
                    str(num): {
                        'start': (
                            self.last_char_position if
                            self.last_char_position == 0 else
                            self.last_char_position + 1
                        ),
                        'end': self.last_char_position + len(data)
                    }
                }
            )

            self.last_char_position += len(data)

    def _write_shard(self, num: int, data: str) -> None:
        """Write an individual database shard to disk and add it to the
        mapping."""
        if not os.path.exists("data"):
            os.mkdir("data")
        with open(f"data/{num}.txt", 'w') as s:
            s.write(data)
        self._write_shard_mapping(str(num), data)

    def _generate_sharded_data(self, count: int, data: str) -> List[str]:
        """Split the data into as many pieces as needed."""
        splicenum, rem = divmod(len(data), count)

        result = [
            data[splicenum * z:splicenum * (z + 1)] for z in range(count)
        ]
        # take care of any odd characters
        if rem > 0:
            result[-1] += data[-rem:]

        return result

    def load_data_from_shards(self) -> str:
        """Grab all the shards, pull all the data, and then concatenate it."""
        result = list()

        for db in self.get_shard_ids():
            with open(f'data/{db}.txt', 'r') as f:
                result.append(f.read())
        return ''.join(result)

    def add_shard(self) -> None:
        """Add a new shard to the existing pool and rebalance the data."""
        self.sync_replication()
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        keys = [int(z) for z in self.get_shard_ids()]
        keys.sort()
        # why 2? Because we have to compensate for zero indexing
        new_shard_num = max(keys) + 2

        spliced_data = self._generate_sharded_data(new_shard_num, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

        self.sync_replication()

    def remove_shard(self) -> None:
        """Loads the data from all shards, removes the extra 'database' file,
        and writes the new number of shards to disk.
        """
        self.sync_replication()
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        keys = [int(z) for z in self.get_shard_ids()]
        if len(keys) == 1:
            raise Exception('Cannot remove last shard')
        keys.sort()
        new_shard_num = max(keys)

        spliced_data = self._generate_sharded_data(new_shard_num, data)

        files = os.listdir('./data')
        for filename in files:
            if '-' not in filename:
                if str(new_shard_num) == filename.split('.')[0]:
                    os.remove(f'./data/{filename}')
            else:
                if str(new_shard_num) == filename.split('-')[0]:
                    os.remove(f'./data/{filename}')

        for key in list(self.mapping):
            if '-' not in key:
                if key == str(new_shard_num):
                    self.mapping.pop(key)
            else:
                if key.split('-')[0] == str(new_shard_num):
                    self.mapping.pop(key)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

        self.sync_replication()

    def add_replication(self) -> None:
        """Add a level of replication so that each shard has a backup. Label
        them with the following format:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        2-1.txt (shard 2, replication 1)
        ...etc.

        By default, there is no replication -- add_replication should be able
        to detect how many levels there are and appropriately add the next
        level.
        """
        self.sync_replication()
        self.replication_level = self.replication_level + 1
        files = os.listdir('./data')
        originals = sorted(
            [filename for filename in files if '-' not in filename])
        for i, file in enumerate(originals):
            source = f'./data/{file}'
            destination = f'./data/{i}-{self.replication_level}.txt'
            copyfile(source, destination)
        keys = self.get_shard_ids()
        for i, key in enumerate(keys):
            self.mapping[f'{i}-{self.replication_level}'] = self.mapping[key]
        self.write_map()
        self.sync_replication()

    def remove_replication(self) -> None:
        """Remove the highest replication level.

        If there are only primary files left, remove_replication should raise
        an exception stating that there is nothing left to remove.

        For example:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        etc...

        to:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        2.txt (shard 2, primary)
        etc...
        """
        self.sync_replication()
        if self.replication_level == 0:
            raise Exception('No replication levels to remove.')
        self.replication_level = self.replication_level - 1
        backup_keys = [key for key in self.get_replication_ids()
                       if int(key.split('-')[1]) > self.replication_level]
        for key in backup_keys:
            self.mapping.pop(key)
            os.remove(f'./data/{key}.txt')
        self.write_map()
        self.sync_replication()

    # def primary_keys_okay(self):
    #     primaries = self.get_shard_ids()
    #     for i, key in enumerate(primaries):
    #         if i != int(key):
    #             return False
    #     if self.mapping[primaries[-1]]['end'] != 3735:
    #         return False
    #     return True

    def primary_files_okay(self):
        files = os.listdir('./data')
        primary_keys = self.get_shard_ids()
        primary_files = sorted(file for file in files if '-' not in file)
        for file, key in zip(primary_files, primary_keys):
            if file.split('.')[0] != key:
                return False
        return len(primary_files) == len(primary_keys)

    def create_replication_dict(self):
        replications = {}
        for key in self.get_replication_ids():
            key_level = int(key.split('-')[1])
            if key_level in replications:
                replications[key_level].append(key)
            else:
                replications[key_level] = [key]
        return replications

    def rep_level_has_same_keys(self, rep_keys, primary_keys, level):
        if len(rep_keys) != len(primary_keys):
            return False
        for rep_key, primary_key in zip(rep_keys, primary_keys):
            if rep_key.split('-')[0] != primary_key:
                return False
        files = [filename for filename in os.listdir(
            './data') if '-' in filename]
        level_files = [filename for filename in files if level ==
                       filename.split('-')[1]]
        if len(level_files) != len(primary_keys):
            return False
        return True

    def update_replicated_levels(self):
        for key in self.get_replication_ids():
            key_name = key.split('-')[0]
            self.mapping[key] = self.mapping[key_name]
        primary_keys = self.get_shard_ids()
        for level, keys in self.create_replication_dict().items():
            if not self.rep_level_has_same_keys(keys, primary_keys, level):
                for primary_key in primary_keys:
                    self.mapping[f'{primary_key}-{level}'] =\
                        self.mapping[primary_key]
                    copyfile(f'./data/{primary_key}.txt',
                             f'./data/{primary_key}-{level}.txt')
        self.write_map()

    def restore_primary_files(self):
        x = len(os.listdir('./data'))
        while not self.primary_files_okay() and x > 0:
            x = x - 1
            files = os.listdir('./data')
            primary_files = []
            other_files = []
            for file in files:
                if '-' not in file:
                    primary_files.append(file)
                else:
                    other_files.append(file)
            primary_file_numbers = [int(file.split('.')[0])
                                    for file in primary_files]
            other_file_numbers = [int(file.split('-')[0])
                                  for file in other_files]
            if primary_files:
                last_primary_file_num = max(primary_file_numbers)
            other_files_num = max(other_file_numbers)
            if not primary_files or last_primary_file_num < other_files_num:
                copyfile(
                    f'./data/{other_files[-1]}',
                    f'./data/{other_files_num}.txt'
                )
            for i, file in enumerate(primary_files):
                if i != int(file.split('.')[0]):
                    for other_file in other_files:
                        if i == int(other_file.split('-')[0]):
                            copyfile(
                                f'./data/{other_file}',
                                f'./data/{i}.txt'
                            )
                            break
        if not self.primary_files_okay():
            raise Exception('Missing files cannot be recoved')

    def sync_replication(self) -> None:
        """Verify that all replications are equal to their primaries and that
        any missing primaries are appropriately recreated from their
        replications."""
        if not self.primary_files_okay():
            self.restore_primary_files()
        self.update_replicated_levels()

    def get_shard_data(self, shardnum=None) -> [str, Dict]:
        """Return information about a shard from the mapfile."""
        if not shardnum:
            return self.get_all_shard_data()
        data = self.mapping.get(shardnum)
        if not data:
            return f"Invalid shard ID. Valid shard IDs: {self.get_shard_ids()}"
        return f"Shard {shardnum}: {data}"

    def get_all_shard_data(self) -> Dict:
        """A helper function to view the mapping data."""
        return self.mapping

    def get_word_at_index(self, index):
        keys = self.get_shard_ids()
        largest_key = max(int(key) for key in keys)
        for key in keys:
            if (
                index >= self.mapping[key]['start']
                and index <= self.mapping[key]['end']
            ):
                file = f'./data/{key}.txt'
                new_index = index - self.mapping[key]['start']
                with open(file, 'r') as f:
                    text = f.read()
                    start_char = text[new_index]
                    while start_char in ' ,.?!";':
                        new_index += 1
                        start_char = text[new_index]
                    string = text[new_index]
                    before_index = new_index
                    after_index = new_index
                    while before_index != 0 and text[before_index] != ' ':
                        before_index -= 1
                        before_char = text[before_index]
                        string = before_char + string
                    if before_index == 0:
                        before_string = self.from_previous_file(
                            f'./data/{str(int(key) - 1)}.txt')
                        string = before_string + string
                    while after_index != len(text) - 1\
                            and text[after_index] != ' ':
                        after_index += 1
                        after_char = text[after_index]
                        string = string + after_char
                    if after_index == len(text) - 1\
                            and key != str(largest_key):
                        after_string = self.from_following_file(
                            f'./data/{str(int(key) + 1)}.txt')
                        string = string + after_string
                    string = string.strip(' ,.?!";').replace(
                        '\n', '').replace('.', '')
                    return (f'{key}.txt', string)

    def from_previous_file(self, file):
        string = ''
        with open(file, 'r') as f:
            text = f.read()
            last_index = len(text) - 1
            last_char = text[last_index]
            while last_char != ' ':
                string = last_char + string
                last_index -= 1
                last_char = text[last_index]
        return string

    def from_following_file(self, file):
        string = ''
        with open(file, 'r') as f:
            text = f.read()
            first_index = 0
            first_char = text[first_index]
            while first_char != ' ':
                string = string + first_char
                first_index += 1
                first_char = text[first_index]
        return string


s = ShardHandler()

s.build_shards(5, load_data_from_file())

print(s.mapping.keys())

s.add_shard()

print(s.mapping.keys())
