import MapReduce
import sys


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
"""This function implements a join query as a MapReduce query."""

def mapper(record):

    key = record[1]
    att = list(record)
    mr.emit_intermediate(key, att)


def reducer(key, l):

    for index in range(1, len(l)):
        mr.emit(l[0]+l[index])

# Do not modify below this line
# =============================


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)