from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from node import Node

PROCESS = "PROCESS"
UNVISITED = "UNVISITED"
VISITED = "VISITED"

class MRIterator(MRJob):
  
  INPUT_PROTOCOL = RawValueProtocol
  OUTPUT_PROTOCOL = RawValueProtocol

  def configure_options(self):
    super(MRIterator, self).configure_options()
    self.add_passthrough_option('--target', help="ID of character")

  def mapper(self, _, line):
    node = Node()
    node.fromLine(line)
    #Processing the starting point 
    if(node.status == PROCESS):
      for connection in node.connections:
        adjNode = Node()
        adjNode.id = connection
        adjNode.distance = int(node.distance) + 1
        #put the node into the stack
        adjNode.status = PROCESS
        if(self.options.target== connection):
          counterName = ("Target ID " + connection + " is connected " + str(adjNode.distance))
          self.increment_counter('Degrees of Separation', counterName, 1)

        yield connection, adjNode.getLine()
      node.status = VISITED
    yield node.id, node.getLine()

  def reducer(self, key, values):
    edges = []
    distance = 9999
    status = UNVISITED

    for value in values:
      node = Node()
      node.fromLine(value)
      #if it's not itself
      if(len(node.connections)>0):
        filtered_connections = filter(None, node.connections)
        edges.extend(filtered_connections)
      if(node.distance < distance):
        distance = node.distance
      if(node.status == VISITED):
        status = VISITED
      if(node.status == PROCESS and status == UNVISITED):
        status = PROCESS
    node = Node()
    node.id = key
    node.distance = distance
    node.status = status
    node.connections = edges

    yield key, node.getLine()

if __name__ == '__main__':
  MRIterator.run()





