CorpusProcessing-Hadoop-Multir
==============================

This is a collection of Hadoop jobs I used to process a Corpus of ~1M documents for input to Multir training.


###/parsing
This component works by taking in a set of tokenized sentences in the input format below

Input Format:

```
SentenceID \t Token1 Token2 Token3 ......\n
1 \t This is an example sentence . \n
2 \t This is another example sentence . \n
```

It runs the CharnianJohnson parser <https://github.com/BLLIP/bllip-parser> 
on a hadoop cluster via hadoop streaming <http://hadoop.apache.org/docs/r1.1.2/streaming.html>

To use the CharniakJohnson parser with Hadoop Streaming you must put the directory in a jar and put that jar on the HDFS. Then when you run the hadoop-streaming app specify its location with the -archives option, this will extract the contained objects into a directory with the same name as the original jar.

/parsing/cjmapper.py is the mapper that calls the CJ executable and handles the I/O
/parsing/run.sh describes how to write the hadoop-streaming command to run the application


###/pre-parse-processing

The pre-parse processing takes in the following information for each sentence:

* Document Name
* Tokens
* Token Offsetes
* Sentence
* Sentence offsets

The Input is in the following format:

```
SentID \t DocumentName \t String of Tokens \t String Of Token Offsets \t Sentence Offsets \t Sentence String
1 \t AFPXXX.txt \t Today 's example of a token string \t 0:5 5:7 8:15 16:18 19:20 21:26 27:33 \t 100 133 \t Today's example of a token string
```


It outputs the following information for each sentence:

* Part Of Speech Tags
* Named Entity Recognition Tags
* Lemmatized token tags


To run this module you should have the following jars:

* hadoop-core-0.20.2-cdh3u1.jar
* stanford-corenlp-1.3.5.jar
* stanford-corenlp-1.3.5-models.jar
* joda-time-2.1.jar


From the /pre-parse-processing directory follow these instructions to run the hadoop job


```mkdir classFolder```


```
javac -source 1.6 -target 1.6 -classpath /path/to/stanford-corenlp-1.3.5.jar:/path/to/org.apache.hadoop/hadoop-core/jars/hadoop-core-0.20.2-cdh3u1.jar:/path/to/stanford-corenlp-1.3.5-models.jar:/path/to/joda-time-2.1.jar -d classFolder src/hadoop/PreParseProcessor.java
```

```
jar -cvf preparseprocessor.jar -C classFolder/ .
```

```
adoop jar preparseprocessor.jar hadoop.PreParseProcessor -libjars /path/to/stanford-corenlp-1.3.5.jar,/path/to/org.apache.hadoop/hadoop-core/jars/hadoop-core-0.20.2-cdh3u1.jar,/path/to/stanford-corenlp-1.3.5-models.jar,/path/to/joda-time-2.1.jar /hdfs/path/to/input /hdfs/path/to/output 100(map tasks) 12(reduce tasks)
```

###/post-parse-processing


The post-parse processing takes in the following information for each sentence:

* Parse String


The input is in the following format:
```
SentenceID \t parseString
0 \t (S1 (NP (NP (NNP BEIJING)) (, ,) (NP (NNP Oct.) (CD 28)) (PRN (-LRB- -LRB-) (NP (NNP Xinhua)) (-RRB- -RRB-))))
```

It outputs the following information for each setnence:

* Dependency Parse

To run this module you should have the following jars:

* hadoop-core-0.20.2-cdh3u1.jar
* stanford-corenlp-1.3.5.jar


From the /post-parse-processing directory follow these instructions to run the hadoop job


```mkdir classFolder```


```
javac -source 1.6 -target 1.6 -classpath /path/to/stanford-corenlp-1.3.5.jar:/path/to/org.apache.hadoop/hadoop-core/jars/hadoop-core-0.20.2-cdh3u1.jar -d classFolder src/hadoop/PostParseProcessor.java
```

```
jar -cvf postparseprocessor.jar -C classFolder/ .
```

```
hadoop jar postparseprocessor.jar hadoop.PostParseProcessor -libjars /path/to/stanford-corenlp-1.3.5.jar,/path/to/org.apache.hadoop/hadoop-core/jars/hadoop-core-0.20.2-cdh3u1.jar /hdfs/path/to/input /hdfs/path/to/output 300(map taskts)
```




