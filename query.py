#!/usr/bin/python
# python 3
import sys, getopt
from itertools import groupby
from operator import itemgetter
import re

def main():
    '''Coding Challenge
    usage: cat datafiles | ./query.py -s {selection} -o {order} -f {filter} -g {group}
    or set up an alias 'query' of this script
    '''

    # parse input options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "s:o:f:g:")
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
    selection = None
    order = None
    filter = None
    group = None
    for op, arg in opts:
        if op == "-s":
            selection = arg.split(',')
        elif op == "-o":
            order = arg.split(',')
        elif op == "-f":
            filter = arg
        elif op == "-g":
            group = arg
        else:
            assert False, "unhandled option"

    # use a dictionary for in-memory data store
    ds = {}
    try:
        while True:
            row = input().split('|')
            key = row[0], row[1], row[3]
            val = row[2], row[4], row[5]
            ds[key] = val
    except EOFError:
        pass

    # set filter string
    revmap = {'STB': 0, 'TITLE': 1, 'DATE': 2, 'PROVIDER': 3, 'REV': 4, 'VIEW_TIME': 5}
    if filter:
        filter = filter.replace('AND', 'and')
        filter = filter.replace('OR', 'or')
        filter = filter.replace('=', '==')
        for k, v in revmap.items():
            filter = filter.replace(k, 'row[' + str(v) + ']')
        # quote DATE string to protect from side-effect of eval()
        filter = re.sub(r'=(\d{4}-\d\d-\d\d)', r'="\1"', filter)

    # select cols and optionally filter out rows
    selected = []
    for key, val in ds.items():
        row = key + val
        if not filter or eval(filter):
            selected.append([row[revmap[sel.split(':')[0]]] for sel in selection])

    # sort by order
    if order:
        selected.sort(key=itemgetter(*[selection.index(x) for x in order if x in selection]))

    # group rows
    if group:
        selected.sort(key=itemgetter(selection.index(group)))
        groups = []
        for _, g in groupby(selected, itemgetter(selection.index(group))):
            groups.append(list(g))
        # apply aggregate functions
        for rows in groups:
            afs = {'min':min,'max':max,'sum':sum,'count':len,'collect':(lambda x: [','.join(x)])}
            row = []
            for sel in selection:
                idx = selection.index(sel)
                col = [r[idx] for r in rows]
                if ':' in sel:
                    fun = afs[sel.split(':')[1]]
                    if fun in [min, max, sum]:
                        c = "%.2f" % (fun(map(lambda x: int(float(x)*100), col))/100)
                    else:
                        c = str(fun(col))
                else:
                    c = col[0]
                row.append(c)
            # format output
            print(','.join(row))

    # no grouping
    else:
        # format output
        for row in selected:
            print(','.join(row))

if __name__ == "__main__":
    main()
