from mrjob.job import MRJob

class MRAverageFriends(MRJob):
  def mapper(self, key, line):
    (ID, name, age, numFriends) = line.split(',')
    yield age, float(numFriends)

  def reducer(self, age, numFriends):
    total = 0
    ne = 0
    #numFriends is a generator.. you can't use len(x) 
    #another way to find the length 
    #sum(i for x in numFriends)
    for i in numFriends:
    	total += i
    	ne+=1
    yield age, total/ne


if __name__ == '__main__':
  MRAverageFriends.run()