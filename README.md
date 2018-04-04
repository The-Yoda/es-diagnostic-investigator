# Elasticsearch diagnostics investigator

## Overview
When issues happen in elasticsearch, it is very hard to debug the root cause of the issue. 

Elastic team has done its best in creating a support tool, through which diagnostic report can be created with minute details.

Analysing the report and identifying the root cause(or probable root cause) of the issue itself is difficult. Elasticsearch diagnostics investigator comes in rescue for such cases.


## Getting Started
### Setting up
### Samples
 
Running with sbt:
 
 `sbt "runMain org.sample.Main -l /Users/jeeva/Desktop/slowlogs/slowlog.log -u http://localhost:9200 -t US/Arizona"`
 
In sbt shell:
  
  sbt> `run  -l "/Users/sujeeva/Desktop/slowlogs/slowlog.log" -u "http://localhost:9200" -t "US/Arizona"`
 