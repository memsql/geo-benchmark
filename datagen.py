#!/usr/bin/env python

import random
import sys
import pickle
from os.path import abspath, dirname, join
from collections import namedtuple


def print_progress_of(iterable, char_width=80, frequency=100):
    n = len(iterable)
    i = 0
    for item in iterable:
        percent_done = (1.0 * i) / n
        bar_length = char_width - 2  # edges
        filled_length = int(round(percent_done * bar_length))
        unfilled_length = bar_length - filled_length
        if i % frequency == 0:
            sys.stdout.write('[%s%s]\r' % (filled_length * '#', unfilled_length * '-'))
            sys.stdout.flush()
        if i == n - 1:
            sys.stdout.write('[%s]\n' % (bar_length * '#'))
            sys.stdout.flush()
        yield item
        i += 1

# namedtuples are pleasant and less memory-intensive
# than their equivalent dictionaries or lists
Row = namedtuple('Row', ['id', 'latitude', 'longitude'])


def main(num_rows=10000000):
    rows = []

    id = 1

    for _ in print_progress_of(xrange(num_rows)):
        row = Row(id, random.uniform(40.64, 40.85), random.uniform(-74.11, -73.30))
        id += 1
        rows.append(tuple(row))

    path = join(dirname(abspath(__file__)), 'data')

    print('Serializing data to disk')
    with open(path, 'w') as f:
        pickle.dump(rows, f)


if __name__ == '__main__':
    main()
