#This script is used to reformat the input data
#to find the degree of separation for the
#hero defined in the argument.
#Usage: data_formatter.py heroID
import sys 

with open("./resources/bfs-0.txt", 'w') as out:
  with open("./dataset/Marvel-graph.txt") as f:
    for line in f:
      fields = line.split()
      heroId = fields[0]
      numConnections = len(fields) - 1;
      connections = fields[-numConnections:]

      status = 'UNVISITED'
      distance = 9999

      if(heroId == sys.argv[1]):
        status = 'PROCESS'
        distance = 0

      if(heroId != ''):
        edges = ','.join(connections)
        #join the tuple/list, join only takes one arguments
        outStr = '|'.join((heroId, edges,str(distance), status))
        out.write(outStr)
        out.write("\n")
  f.close()
out.close()

