from pyspark import SparkContext, StorageLevel
from collections import Counter
from operator import add
import json
import os
import time
import sys
import itertools
import math
import csv

#COMMANDLINE ARGUMENTS
case = sys.argv[1]; case = int(case);
support = sys.argv[2]; supportThreshold = int(support); 
input_file_path = sys.argv[3]; 
output_file_path = sys.argv[4]; 

# +++++++++++++++++++++++++++++++++++++++++++++++++++ Writing into file +++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def writing_into_file(heading, data, mode):
    f = open(output_file_path, mode)
    data.sort()
    data = sorted(data, key=len)
    content = ""
    if mode == "w":
        content = heading + ":\n"
    else:
        content = "\n\n" + heading + ":\n"
    length = 1
    for item in data:
        if (len(item) == 1):
            for i in item:
                content += "('" + i + "'),"
        else:
            if length != len(item):
                content = content[0:len(content) - 1]
                content += "\n\n"
                length = len(item)
            content += str(item).replace("u'", "'")
            content += ","
    if (content[len(content) - 1] == ","):
        content = content[0:len(content) - 1]

    f.write(content)
    f.close()


# +++++++++++++++++++++++++++++++++++++++++++++++++++ 2. APRIORI ALGORITHM (Map 1) +++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def apriori_algorithm(basketschunk, supportThreshold, total_no_of_baskets):
    baskets = dict()
    basket_list = list()
    for b in basketschunk:
        baskets[b[0]] = b[1]
        basket_list.append(b[1])

    chunk_no_of_baskets = len(basket_list)
    partition_support_threshold = (float(chunk_no_of_baskets) / float(total_no_of_baskets)) * supportThreshold

    items = dict()
    for key in basket_list:
        # print(key)
        for i in key:
            if i in items:
                items[i] += 1
            else:
                items[i] = 1
    items = list(items.items())
    items.sort()
    all_freq_itemsets_list = []
    freq_items = dict()
    for key, value in items:
        if value >= partition_support_threshold:
            freq_items[key] = value
            all_freq_itemsets_list.append((key,))

    freq_items_list = []
    for f in freq_items:    freq_items_list.append(f)
    singletons = set(freq_items_list)
    currentItemtons = list()
    print("SINGETONS DONE")

    # PASS 2 ONWARDS
    pass_no = 2
    if_more_freq_itemsets = 1  # 1 indicates there are still freq itemsets to be found, = 0 indicates no more freq itemsets can be found
    itemtonsFreq = set(singletons)
    while (if_more_freq_itemsets != 0):
        print("ENTERED pass_no:", pass_no)

        if_more_freq_itemsets = 0
        combinations = []
        if (pass_no == 2):
            
            combinations = itertools.combinations(freq_items_list, pass_no)
            print("PAIRS - CANDIDATES - DONE")
        else:

            # GET CANDIDATE ITEMS
            k_freqItems = list(itemtonsFreq)  

            for f in range(len(k_freqItems) - 1):
                for j in range(f + 1, len(k_freqItems)):
                    itemset1 = k_freqItems[j]
                    itemset2 = k_freqItems[f]
                    if itemset1[0:(pass_no - 2)] == itemset2[0:(pass_no - 2)]:
                        combinations.append(tuple(set(itemset1) | set(itemset2)))
                    else:
                        break

        if (combinations != None):
            pass_no_item_count = dict()
            for c in combinations:
                c = set(c)
                key = tuple(sorted(c))

                for b in basket_list:
                    if c.issubset(b):
                        if key in pass_no_item_count:
                            pass_no_item_count[key] = pass_no_item_count[key] + 1
                        else:
                            pass_no_item_count[key] = 1

            kCount = Counter(pass_no_item_count)
            currentItemtons = dict()
            for z in kCount:
                if partition_support_threshold <= kCount[z]:
                    currentItemtons[z] = kCount[z]
            currentItemtons = sorted(currentItemtons)

            # for new in currentItemtons:
            for new in currentItemtons:
                all_freq_itemsets_list.append(new)
            itemtonsFreq = list(set(currentItemtons))
            itemtonsFreq.sort()
            if (len(itemtonsFreq) != 0):
                if_more_freq_itemsets = 1

            print("ITEMSETS - FREQUENT - DONE ", pass_no)
            pass_no += 1
            
    return all_freq_itemsets_list


# +++++++++++++++++++++++++++++++++++++++++++++++++++ Map 2  +++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def counting_freq_cands(basket, candidate_itemsets):

    freq_itemsets = []
    for item in candidate_itemsets:
        # print ("ITEM:", item)
        if type(item) == str:
            if item in basket:
                freq_itemsets.append((item, 1))
        elif type(item) == tuple:
            ch = 1
            for t in item:
                if t not in basket:
                    ch = 0
            if ch == 1:
                freq_itemsets.append((item, 1))

    return freq_itemsets


# +++++++++++++++++++++++++++++++++++++++++++++++++++ 1. SON ALGORITHM +++++++++++++++++++++++++++++++++++++++++++++++++++++++++


def SON_Algorithm(basketsRDD, supportThreshold, total_no_of_baskets):
    no_of_partitions = basketsRDD.getNumPartitions()
    partition_distribution = basketsRDD.glom().map(lambda partition: len(partition)).collect()
    data_in_partition = basketsRDD.glom().collect()
    print("1. NUMBER OF PARTITIONS:", no_of_partitions)
    print("2. PARTITION DISTRIBUTION:", partition_distribution)
    print("3. DATA IN PARTITION DISTRIBUTION:");

    map_phase_1 = basketsRDD.mapPartitions(lambda basketchunk: apriori_algorithm(basketchunk, supportThreshold, total_no_of_baskets)).map(lambda e: (e, 1))  
    #print ("Map 1 done")
    # mapPartitions - MUST RETURN A LIST
    # reduce_phase_1
    reduce_phase_1 = map_phase_1.reduceByKey(lambda v1, v2: v1 + v2).map(lambda e: e[0])

    SON_candidate_frequent_itemsets = reduce_phase_1.collect()

    # WRITING CANDIDATE ITEMSETS INTO FILE
    writing_into_file("Candidate", SON_candidate_frequent_itemsets, "w")

    # map_phase_2
    map_phase_2 = basketsRDD.flatMap(lambda entry: counting_freq_cands(entry[1], SON_candidate_frequent_itemsets))

    # reduce_phase_2
    reduce_phase_2 = map_phase_2.reduceByKey(lambda v1, v2: v1 + v2)

    SON_frequent_itemsets = reduce_phase_2.filter(lambda e: e[1] >= supportThreshold).map(lambda e: e[0]).collect()

    # WRITING FREQUENT ITEMSETS INTO FILE
    writing_into_file("Frequent Itemsets", SON_frequent_itemsets, "a")

# +++++++++++++++++++++++++++++++++++++++++ MAIN METHOD ++++++++++++++++++++++++++++++++++++++++++

SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'task1')

# start time
start = time.time()

currentRDD = sc.textFile(input_file_path).map(lambda e: e.split(",")).map(lambda e: (e[0], e[1]))
head = currentRDD.take(1)
dataRDD = currentRDD.filter(lambda e: e[0] != head[0][0])
if case == 2:
    basketRDD = dataRDD.map(lambda e: (e[1], e[0])).groupByKey().mapValues(lambda e: list(set(e)))
elif case == 1:
    basketRDD = dataRDD.map(lambda e: (e[0], e[1])).groupByKey().mapValues(lambda e: list(set(e)))  # SET removes duplicates. CHECK - can remove list()

total_no_of_baskets = basketRDD.count()


SON_Algorithm(basketRDD, supportThreshold, total_no_of_baskets)


# end time
end = time.time()
print ("Duration:", end - start)


