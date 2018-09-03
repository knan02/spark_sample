import sys
import argparse
from pyspark import SparkContext, SparkConf
 
if __name__ == "__main__":


  parser = argparse.ArgumentParser()
  parser.add_argument("-i", "--input_requests", type = str, required=True, help = "input path for request logs")
  parser.add_argument("-m", "--idemo_request", type = str, required=True, help = "input path for meta logs")
  args = parser.parse_args()    
  
  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Word count in pyhton")
  sc = SparkContext(conf=conf)
 
  
  # read in text file and split each document into words
  words = sc.textFile(args.input_requests).flatMap(lambda line: line.split(" "))
 
  # count the occurrence of each word
  wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
 
  wordCounts.saveAsTextFile(args.idemo_request)
