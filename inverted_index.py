import MapReduce
import sys


mr = MapReduce.MapReduce()
#This is a global variable named mr which points to a MapReduce object.


def mapper(record):
    """This function input a 2 element list (document_id, document)."""
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = set(value.split())
    for w in words:
        mr.emit_intermediate(w, key)


def reducer(key, list_of_values):
    """The output is a (word, document ID list) tuple where word is a String 
    and document ID list is a list of Strings."""
    # key: word
    # value: list of document identifiers
    document_id = []
    for v in list_of_values:
        document_id.append(v)
    mr.emit((key, document_id))


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)