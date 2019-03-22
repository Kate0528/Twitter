import MapReduce
import sys



mr = MapReduce.MapReduce()

"""
Assume we have two matrices A and B in a sparse matrix format, where each record is of the form i, j, value. 
Design a MapReduce algorithm to compute the matrix multiplication A x B
"""

def mapper(record):

    matrix, row, col, value = record
    for n in range(5):
        if matrix == 'a':
            cell = (row, n)
            mat = 'a'
            col_row = col
        elif matrix == 'b':
            cell = (n, col)
            mat = 'b'
            col_row = row
        mr.emit_intermediate(cell, (mat, col_row, value))


def reducer(key, l):

    matrix_a = [(item[1], item[2]) for item in l if item[0] == 'a']
    matrix_b = [(item[1], item[2]) for item in l if item[0] == 'b']
    result = 0

    for A_item in matrix_a:
        for B_item in matrix_b:
            if A_item[0] == B_item[0]:
                result += A_item[1] * B_item[1]

    if result != 0:
        mr.emit((key[0], key[1], result))



if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)