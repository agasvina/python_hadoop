from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
  """Mappper-Reducer for finding the most popular movie (Based on rating)"""

  def configure_options(self):
    super(MostPopularMovie, self).configure_options()
    self.add_file_option("--item", help="path to movie_title's dataset")

  def steps(self):
    return [
      MRStep(mapper=self.get_rating_mapper, reducer_init=self.reducer_init, reducer=self.count_ratings_reducer),
      MRStep(mapper=self.mapper_pipeline, reducer=self.most_popular_reducer)
    ]

  def get_rating_mapper(self, _, line):
    (userID, movieID, rating, timestamp) = line.split('\t')
    yield movieID, float(rating)

  def reducer_init(self):
    self.movieTitle = {}
    with open("u.ITEM") as file:
      for line in file:
        fields = line.split('|')
        self.movieTitle[fields[0]] = fields[1].decode('utf-8','ignore')

  def count_ratings_reducer(self, movieID, ratings):
    total = 0
    timesRate = 0
    for i in ratings:
      total+= i
      timesRate+= 1
    yield None, ((total/timesRate), self.movieTitle[movieID])

  #just a pipeline to avoid bug in mrjob.
  #this won't normally be needed
  def mapper_pipeline(self, key, movie_tuples):
    yield key, movie_tuples

  def most_popular_reducer(self, key, movie_tuples):
    yield max(movie_tuples)

if __name__ == '__main__':
    MostPopularMovie.run()



