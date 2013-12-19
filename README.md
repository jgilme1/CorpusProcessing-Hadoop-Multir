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



###/post-parse-processing
