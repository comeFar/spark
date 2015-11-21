class RDD(object):
  def __init__(self, id):
    self.partitions = []
    self.id = id

  def collect(self):
    return self.partitions

  def count(self):
    return len(self.collect())

  def is_number(self, var):
    try:
        if var == str(int(var)) or var == str(float(var)):
            return True
    except Exception:
        return False

  def textFile(self, file_name):
    result = RDD(self.id+1)
    f = open(file_name)
    result.partitions = f.readlines()
    f.close()
    return result

  def map(self, func):
    result = RDD(self.id+1)
    for content in self.partitions:
      result.partitions.append(func(content))
    return result

  def filter(self, func):
    result = RDD(self.id+1)
    for index,content in enumerate(self.partitions):
      if func(content):
        result.partitions.append(content)
    return result

  def reduceByKey(self, func):
    result = RDD(self.id+1)
    dict = {}
    for content in self.partitions:
      k = content[0]
      v = content[1]
      if k in dict:
        if self.is_number(dict[k]) and self.is_number(v):
          dict[k] = int(dict[k])
          v = int(v)
        dict[k] = func(dict[k], v)
        dict[k] = str(dict[k])
      else:
        dict[k] = v
    for key, value in dict.iteritems():
      temp = [key, value]
      result.partitions.append(temp)
    return result

  def groupByKey(self):
    result = RDD(self.id+1)
    dict = {}
    for content in self.partitions:
      k = content[0]
      v = content[1]
      if k in dict:
        dict[k].append(v)
      else:
        dict[k] = [v]
    for key, value in dict.iteritems():
      temp = [key, value]
      result.partitions.append(temp)
    return result

if __name__ == '__main__':
  r = RDD(1)
  # print r.textFile('myfile').map(lambda s: s.split()).filter(lambda a: int(a[1]) > 2).reduceByKey(lambda a, b: a + b).collect()
  print r.textFile('myfile').map(lambda s: s.split()).filter(lambda a: int(a[1]) > 2).groupByKey().collect()