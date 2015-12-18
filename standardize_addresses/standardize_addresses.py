"""Standardize street addresses.
"""

import argparse
import csv
import numpy as np
import os
import pandas as pd
import re


def parse_args():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser(
        description='Standardize street addresses.')
    parser.add_argument(
        '--input_file', required=True,
        help='file containing data to standardize')
    parser.add_argument(
        '--output_file', required=True,
        help='file to which standardized data is to be written')
    parser.add_argument(
        '--housenum_column', required=True,
        help='column containing house number data to standardize')
    parser.add_argument(
        '--street_column', required=True,
        help='column containing street data to standardize')
    parser.add_argument('--sep', default=',', help='column separator')
    parser.add_argument(
        '--chunksize', type=int, default=100000,
        help='number of records to process at a time')
    return parser.parse_args()


def read_csv(filename, sep, chunksize):
    """Read delimited data from file.

    Parameters
    ----------
    filename : str
        name of the file from which to read data
    sep : str
        column separator
    chunksize : int
        number of records to read at a time

    Returns
    -------
    reader : pandas.io.parsers.TextFileReader
        iterator yielding chunksize-sized dataframes over data
    """
    return pd.read_csv(
        filename, encoding='utf-8', dtype=str, sep=sep,
        quoting=csv.QUOTE_NONE, na_filter=False, chunksize=chunksize)


def preprocess(data):
    preprocessed = data.upper().strip()
    consecutive_spaces = re.compile(r'\s\s')
    while consecutive_spaces.search(preprocessed):
        preprocessed = consecutive_spaces.sub(' ', preprocessed)
    return preprocessed


def standardize_housenum(housenum):
    housenums = [hn for hn in re.split(r'\s*&\s*|\s*,\s*|\s+AND\s+|\s+OR\s+', housenum) if len(hn)]
    additional_housenums = ', '.join(housenums[1:]) if housenums[1:] else ''
    housenum_std = re.sub('[^?0-9R\.]', '', housenums[0])
    partial = ''
    if housenum_std.find('?') >= 0:
       partial = 'P'
    rear  = ''
    if housenum_std.find('R') >= 0:
        rear = 'R'
        housenum_std = housenum_std.replace('R', '')
    housenum_std = re.sub('^\.+', '', housenum_std)
    housenum_std = re.sub('\..*$', '', housenum_std)
    return (housenum_std, partial, rear, additional_housenums)


def standardize_street(street):
    directionals = {'N': 'N',
                    'NE': 'NE',
                    'E': 'E',
                    'SE': 'SE',
                    'S': 'S',
                    'SW': 'SW',
                    'W': 'W',
                    'NW': 'NW',
                    'NORTH': 'N',
                    'NORTHEAST': 'NE',
                    'EAST': 'E',
                    'SOUTHEAST': 'SE',
                    'SOUTH': 'S',
                    'SOUTHWEST': 'SW',
                    'WEST': 'W',
                    'NORTHWEST': 'NW'}
    suffixes = {'ALLEE': 'ALLEY',
                'ALLEY': 'ALLEY',
                'ALLY': 'ALLEY',
                'ALY': 'ALLEY',
                'AV': 'AVE',
                'AVE': 'AVE',
                'AVEN': 'AVE',
                'AVENU': 'AVE',
                'AVENUE': 'AVE',
                'AVN': 'AVE',
                'AVNUE': 'AVE',
                'HIGHWAY': 'HWY',
                'HIGHWY': 'HWY',
                'HIWAY': 'HWY',
                'HIWY': 'HWY',
                'HWAY': 'HWY',
                'HWY': 'HWY',
                'PL': 'PL',
                'PLACE': 'PL',
                'RD' : 'RD',
                'ROAD': 'RD',
                'STREET': 'ST',
                'STRT': 'ST',
                'ST': 'ST',
                'STR': 'ST'}
    streets = [s for s in re.split(r'\s*&\s*|\s*,\s*|\s+AND\s+|\s+OR\s+', street) if len(s)]
    additional_streets = ', '.join(streets[1:]) if streets[1:] else ''
    street_parts = streets[0].split()
    if streets[0] in ['ILLEGIBLE', 'NO']:
        street_parts = []
    names = []
    suffix = ''
    directional = ''
    for (index, street_part) in enumerate(street_parts):
        street_part = re.sub('[^A-Z0-9]', '', street_part)
        if street_part == 'ST' and index == 0 and len(street_parts) > 1:
           names.append(street_part)
        elif street_part in directionals:
            directional = directionals[street_part]
        elif street_part in suffixes:
            suffix = suffixes[street_part]
        elif len(street_part) and street_part != 'ILLEGIBLE' and street_part != 'NO':
            names.append(street_part)
    return (' '.join(names), suffix, directional, additional_streets)


def standardize_addresses(dataframe, housenum_column, street_column):
    """Standardize street addresses.

    Parameters
    ----------
    dataframe : pandas.DataFrame
        data to standardize
    housenum_column : str
        name of the column having house number data to standardize
    street_column : str
        name of the column having street data to standardize

    Returns
    -------
    standardized : pandas.DataFrame
        dataframe with standardized addresses
    """
    u_preprocess = np.frompyfunc(preprocess, 1, 1)
    standardize_housenums = np.frompyfunc(standardize_housenum, 1, 4)
    standardize_streets = np.frompyfunc(standardize_street, 1, 4)
    standardized = dataframe.copy()
    standardized[housenum_column] = u_preprocess(standardized[housenum_column])
    standardized[street_column] = u_preprocess(standardized[street_column])
    (standardized['HOUSENUM_STD'],
     standardized['PARTIAL'],
     standardized['REAR'],
     standardized['ADDITIONAL_HOUSENUMS']) = \
        standardize_housenums(standardized[housenum_column])
    (standardized['STREET_NAME'],
     standardized['STREET_SUFFIX'],
     standardized['STREET_DIRECTIONAL'],
     standardized['ADDITIONAL_STREETS']) = \
        standardize_streets(standardized[street_column])
    return standardized


def main():
    """Parse commandline arguments, read data, standardize addresses,
    and write standardized data."""
    args = parse_args()
    assert not os.path.exists(args.output_file), \
        'output file ({output_file}) ' \
        'already exists'.format(output_file=args.output_file)
    reader = read_csv(args.input_file, args.sep, args.chunksize)
    for dataframe in reader:
        standardized = standardize_addresses(dataframe, args.housenum_column, args.street_column)
        mode = 'a' if os.path.exists(args.output_file) else 'w'
        header = (mode == 'w')
        standardized.to_csv(args.output_file,
                            encoding='utf-8',
                            mode=mode,
                            header=header,
                            sep=args.sep,
                            index=False,
                            quoting=csv.QUOTE_NONE)


if __name__ == '__main__':
    main()
