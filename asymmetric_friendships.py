import MapReduce
import sys

"""
The relationship "friend" is often symmetric, meaning that if I am your friend, you are my friend. 
Implement a MapReduce algorithm to check whether this property holds. 
Generate a list of all non-symmetric friend relationships.
"""

mr = MapReduce.MapReduce()


def mapper(record):

    pair = sorted(record)
    pair = tuple(pair)
    mr.emit_intermediate(pair,1)


def reducer(key, occur):

    if len(occur) < 2:
        mr.emit(key)



if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)