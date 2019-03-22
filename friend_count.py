import MapReduce
import sys


mr = MapReduce.MapReduce()

"""
This MapReduce algorithm is used to count the number of friends for each person.
"""


def mapper(record):

    key = record[0]
    mr.emit_intermediate(key, 1)


def reducer(person, friends):

    friend_count = len(friends)
    mr.emit((person, friend_count))



if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)