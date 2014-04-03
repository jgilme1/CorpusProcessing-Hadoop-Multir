#!/usr/bin/env python

import sys
import os
from subprocess import Popen, PIPE, STDOUT

tokenSequences = []
reducerOutput = []

cjprefix = "<s> "
cjsuffix = " </s>"
wd  = os.getcwd()
targetcd = os.getcwd()+"/bllip-parser.jar/"
#targetcd = os.getcwd()+"/../bllip-parser/"
cjParserArguments = ["./parse.sh", "-K", "-T50","-S"]

# takes in (id\ttokens) format
# runs local cj parser
def parseTokens(tokenSequences):
  cjFormattedTokenSequences = []
  startID = -1
  endID = -1
  for tokenSequence in tokenSequences:
    #values = tokenSequence.split('\t',1)
    #key = int(values[0])
    #tokens = values[1]
    #reducerOutput.append(str(key)+"\t"+tokens)
   
    # serialize tokens in cj format
    values = tokenSequence.split('\t',1)
    sentID = int(values[0])
    endID = sentID
    if (startID == -1):
      startID = sentID
    tokens = values[1]
    cjString = cjprefix+tokens+cjsuffix
    cjFormattedTokenSequences.append(cjString)
 # sys.stderr.write(targetcd + "\n")
 # sys.stderr.write(wd + "\n")
 # dirlist = os.listdir(targetcd)
 # for dir in dirlist:
 #  sys.stderr.write(dir+"\n")

  #sys.stderr.write(check_output(["ls","-l"])+"\n")
  #sys.stderr.write(check_output(["ls","-l"],cwd=targetcd)+"\n")

  p = Popen(cjParserArguments, stdout=PIPE, stdin=PIPE, stderr=STDOUT,cwd=targetcd)
  
  cjInput = ""
  for cjFormattedTokenSequence in cjFormattedTokenSequences:
    cjInput = cjInput+cjFormattedTokenSequence+"\n"

  #print(cjInput)
  # send the process the std input and get back standard output
  sys.stderr.write("Parsing tokens\n")
  cjOutput = p.communicate(input=cjInput)[0].strip()
  sys.stderr.write("Parsed tokens\n")
  
  #parse output
  i = 0
  for line in cjOutput.split("\n"):
    if line.startswith("("):
      sentid = startID + i
      parse = line
      reducerOutput.append(str(sentid)+"\t"+parse)
      i += 1 
  
sys.stderr.write("STARTING MAP TASK\n")
for line in sys.stdin:
  tokenSequences.append(line)

parseTokens(tokenSequences)

#reducer output
for ro in reducerOutput:
  values = ro.split('\t',1)
  key = int(values[0])
  parseString = values[1]
  print '%s\t%s' % (key,parseString)
