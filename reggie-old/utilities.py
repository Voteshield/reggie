import re

def date_from_str(s):
    """
    return the <date> component from a filename or s3 key following the
    standard format:

        voter_file/<state>/<source>/<date>.<file_extension>.gz

    :return date string
    """
    match = re.search(r'(\d+[-_/]\d+[-_/]\d+)', s) if s is not None else None
    if match is not None:
        date = str(match.group(1))
    else:
        date = None
    return date