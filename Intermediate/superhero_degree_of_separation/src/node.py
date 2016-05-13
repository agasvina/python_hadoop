class Node:
  #initial value when a Node is created
  def __init__(self):
    self.id = ''
    self.connections = []
    self.distance = 9999
    self.status = 'UNVISITED'

  #set the node value from line
  #Format is ID|EDGES|DISTANCE|STATUS
  def fromLine(self, line):
    fields = line.split('|')
    if(len(fields) == 4):
      self.id = fields[0]
      self.connections = fields[1].split(',')
      self.distance = int(fields[2])
      self.status = fields[3]

  def getLine(self):
    connections = ','.join(self.connections)
    return '|'.join((self.id, connections, str(self.distance), self.status))

