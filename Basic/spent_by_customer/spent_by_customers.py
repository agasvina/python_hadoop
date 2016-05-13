from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSpentByCustomer(MRJob):
  """Mapper-Reducer for knowing the average amount spent by customers"""
  def steps(self):
    return [
      MRStep(mapper=self.mapper_get_avg, reducer=self.reducer_count_avg),
      MRStep(mapper=self.mapper_make_avg_key, reducer= self.output)
    ]

  def mapper_get_avg(self, _, line):
    (customer, item, order_amount) = line.split(',')
    yield customer, float(order_amount)

  def reducer_count_avg(self, customer, orders):
    total = 0
    num_orders = 0
    for i in orders:
      total += i
      num_orders+=1
    yield customer, (total/num_orders)

  def mapper_make_avg_key(self, customer, avg):
    yield '%04.02f'%float(avg), customer

  def output(self, avg, customers): 
    for cust_id in customers:
      yield cust_id, avg
    
if __name__ == '__main__':
    MRSpentByCustomer.run()