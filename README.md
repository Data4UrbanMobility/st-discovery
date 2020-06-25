# ST-Discovery

## Contents

This repository contains the implementation of the ST-Discovery approach for identifying structural dependencies within road networks.

## Prerequisites

Java 11

Maven

You can then create a maven projet using the provided pom.xml file.

## Runing
You can run the Application class and provide one or more configuration files as an argument.

## Configuration Parameters
The following configuration parameters can be set in the configuration file. Parameters are set in the form of key = value, one parameter per line.

dbHost  -  IP of the Postgre database

dbUser  -   Name of the database use

dbPassword -    Password of the database user

dbName -    Name of the database

dbMaxConnections -  Number of maximum allowed simultaneous connections

dbSchema - Target schema in the database


inputTable - Table containing traffic data

idColumn - Column containing the street id in inputTable

speedColumn - Column containing the traffic speed in inputTable

timeColumn - Column containing the time stamp in inputTable

outlierTable - Table to store/load traffic outliers

outlierIdentification - true/false, whether to compute or load outliers

graphTable - Table containig a pgrouting graph

subgraphTable - Table containing subgraphs with names

subgraphName - Name of the subgraph to use, can be left out if the whole graph should be used

thSim - Similarity threshold for the spatial merging

writeRegions - true/false whether to write regions determined by st-discovery to a tsv file

thdmin - Minimum distance for dependcy calculation

## Publications
Tempelmeier, N., Feuerhake, U., Wage, O. & Demidova, E. (2019). ST-Discovery: Data-Driven Discovery of Structural Dependencies in Urban Road Networks. In Proc. of 27th ACM SIGSPATIAL International Conference on Advances in Geographic Information Systems (ACM SIGSPATIAL 2019) . ACM.

Cite as:
```
@conference{tempelmeier2019stdiscovery,
    author = {Tempelmeier, Nicolas and Feuerhake, Udo and Wage, Oskar and Demidova, Elena},
    booktitle = {Proc. of 27th ACM SIGSPATIAL International Conference on Advances in Geographic Information Systems (ACM SIGSPATIAL 2019)},
    keywords = {campaneo data4urbanmobility myown tempelmeier},
    publisher = {ACM},
    timestamp = {2019-10-09T22:23:42.000+0200},
    title = {ST-Discovery: Data-Driven Discovery of Structural Dependencies in Urban Road Networks},
    year = 2019
}
```








