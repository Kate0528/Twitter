import MapReduce
import sys



mr = MapReduce.MapReduce()

"""
Consider a set of key-value pairs where each key is sequence id and each value is a string of nucleotides, e.g., GCTTCCGAAATGCTCGAA....
Write a MapReduce query to remove the last 10 characters from each string of nucleotides, then remove any duplicates generated.
"""

def mapper(record):

    key = record[1][:-10]
    mr.emit_intermediate(key, 1)


def reducer(key, value):

    mr.emit(key)



if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)