from pyspark import SparkContext, StorageLevel
from operator import add
import json
import os
import time
import sys
import itertools
import math

#variables
review_file_path = "review.json";
business_file_path = "business.json";
output_file_path = "user_business.csv";


#start time
start = time.time()
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'preprocess')

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

reviewsRDD = sc.textFile(review_file_path).map(lambda x: json.loads(x)).map(lambda entry: (entry['business_id'], entry['user_id'])).persist(storageLevel=StorageLevel.MEMORY_ONLY)
print("READ REVIEWS")

businessRDD = sc.textFile(business_file_path).map(lambda x: json.loads(x)).map(lambda entry: (entry['business_id'], entry['state'])).persist(storageLevel=StorageLevel.MEMORY_ONLY)
print("READ BUSINESS")


user_business = reviewsRDD.join(businessRDD).filter(lambda e: e[1][1] == "NV").map(lambda e: (e[1][0], e[0])).collect()

f = open(output_file_path, "w")
f.write("user_id,business_id" + "\n")
for i in range (len(user_business)):
    if (i != len(user_business) - 1):
        f.write(user_business[i][0] + ","+ user_business[i][1]+ "\n")
    else:
        f.write(user_business[i][0] + ","+ user_business[i][1])
f.close()



#end time
end = time.time()
print ("Duration:", end - start)




