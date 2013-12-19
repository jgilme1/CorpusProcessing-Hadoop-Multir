#!/bin/bash

#hadoop jar ~knowall/hadoop/contrib/streaming/hadoop-streaming-1.0.2.jar -D mapred.reduce.tasks=0 -D mapred.map.tasks=100 -D mapred.task.timeout=3600000 -archives 'hdfs://rv-n11.cs.washington.edu:9000/user/jgilme1/cjparser/bllip-parser.jar' -input /user/jgilme1/cjparser/sentences.tokens.180k -output /user/jgilme1/cjparser/sentences.parses.noreduce.180k.2 -file cjmapper2.py -mapper "python cjmapper2.py"

#hadoop jar /path/to/hadoop-streaming-1.0.2.jar -D mapred.reduce.tasks=0 -D mapred.map.tasks=x -D mapred.task.timeout=LONGTIMEOUT -archives /hdfs/path/to/bllip-parser.jar' -input /hdfs/path/to/tokenized/sentences -output /hdfs/path/to/parsed/output -file cjmapper.py -mapper "python cjmapper.py"
