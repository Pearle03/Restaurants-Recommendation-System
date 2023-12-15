import argparse
import collections
import csv
import json
import collections.abc


def read_and_write_file(json_file_path, csv_file_path, column_names):
    """Read in the json dataset file and write it out to a csv file, given the column names."""
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as fout:  # Open in text mode
        csv_file = csv.writer(fout)
        csv_file.writerow(list(column_names))
        with open(json_file_path, 'r', encoding='utf-8') as fin:  # Ensure to open in read mode
            for line in fin:
                line_contents = json.loads(line)
                csv_file.writerow(get_row(line_contents, column_names))


def get_superset_of_column_names_from_file(json_file_path):
    """Read in the json dataset file and return the superset of column names."""
    column_names = set()
    with open(json_file_path) as fin:
        for line in fin:
            line_contents = json.loads(line)
            column_names.update(
                set(get_column_names(line_contents).keys())
            )
    return column_names


def get_column_names(line_contents, parent_key=''):
    column_names = []
    for k, v in line_contents.items():  # Use .items() for Python 3
        column_name = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, collections.abc.MutableMapping):
            column_names.extend(
                get_column_names(v, column_name).items()
            )
        else:
            column_names.append((column_name, v))
    return dict(column_names)


def get_nested_value(d, key):
    # Check if d is a dictionary and key is a string
    if not isinstance(d, dict) or not isinstance(key, str):
        return None

    if '.' not in key:
        return d.get(key)  # Safely get the value, returns None if key is not present

    base_key, sub_key = key.split('.', 1)
    sub_dict = d.get(base_key)  # Safely get the nested dictionary

    if sub_dict is None:
        return None

    return get_nested_value(sub_dict, sub_key)


def get_row(line_contents, column_names):
    """Return a csv compatible row given column names and a dict."""
    row = []
    for column_name in column_names:
        line_value = get_nested_value(
            line_contents,
            column_name,
        )
        if isinstance(line_value, str):
            row.append(f'"{line_value}"')
        elif line_value is not None:
            row.append(f'"{line_value}"')
        else:
            row.append('')
    return row


if __name__ == '__main__':
    """Convert a yelp dataset file from json to csv."""

    parser = argparse.ArgumentParser(
        description='Convert Yelp Dataset Challenge data from JSON format to CSV.',
    )

    parser.add_argument(
        'json_file',
        type=str,
        help='The json file to convert.',
    )

    args = parser.parse_args()

    json_file = args.json_file
    csv_file = '{0}.csv'.format(json_file.split('.json')[0])

    column_names = get_superset_of_column_names_from_file(json_file)
    read_and_write_file(json_file, csv_file, column_names)
