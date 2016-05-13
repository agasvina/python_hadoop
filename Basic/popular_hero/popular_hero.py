from mrjob.job import MRJob
from mrjob.step import MRStep
 
class MostPopularHero(MRJob):
  """Mappper-Reducer for finding the most popular Hero"""

  def configure_options(self):
    super(MostPopularHero, self).configure_options()
    self.add_file_option("--item", help="path to movie_title's dataset")

  def steps(self):
    return [
      MRStep(mapper=self.get_rating_mapper,
            reducer_init=self.reducer_init,
            reducer=self.count_friends_reducer),
      MRStep(mapper=self.mapper_pipeline, reducer=self.most_popular_reducer)
    ]

  def get_rating_mapper(self, _, line):
    fields = line.split();
    yield fields[0], len(fields) - 1

  def reducer_init(self):
    self.heroNames = {}
    with open("Marvel-Names.txt") as file:
      for line in file:
        fields = line.split('"')
        self.heroNames[int(fields[0])] = unicode(fields[1], errors='ignore')

  def count_friends_reducer(self, name, friends):
    heroAlias = self.heroNames[int(name)]
    yield None, (sum(friends), heroAlias)

  #just a pipeline to avoid bug in mrjob.
  #this won't normally be needed
  def mapper_pipeline(self, key, hero_tuples):
    yield key, hero_tuples

  def most_popular_reducer(self, key, hero_tuples):
    yield max(hero_tuples)

if __name__ == '__main__':
    MostPopularHero.run()



