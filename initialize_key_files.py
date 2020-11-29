import json
import os
import sys


if __name__ == '__main__':
    encoded_key_files = os.getenv('KEY_FILES_ENCODED', default=None)
    if encoded_key_files is None:
        sys.exit(1)

    key_files = json.loads(encoded_key_files)
    for key_file_name, contents in key_files.items():
        with open(f"/home/hummingbot/conf/{key_file_name}", 'w') as f:
            f.write(json.dumps(contents))

