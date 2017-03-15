# HotspotAnalysis
Submitted by Group-19	

Nikhil Vementala,  1209370509, nvementa@asu.edu

Nikhil Meka,       1209311086, nemka@asu.edu    	

Kalyan Juluru,     1209320134, kjulru@asu.edu   	

Yogananda Kishore, 1209385655, yyalugot@asu.edu 	

Ramesh Bugatha,    1209390699, rbugatha@asu.edu 


Problem Statement:
Given a dataset comprising of New York City Yellow Cab taxi records from the month of January 2015, return the list of most significant hot spot cells in time and space by using GetisOrd G statistic.

Input :

Input is Newyork yellow taxi route data which will have information about Trip pickup, drop location and time. It is in csv format. We have loaded dataset into HDFS.

For this phase, we are considering only specific trip details like PickUp location, trip count per location and Date of Pickup to calculate our statistic value.


Output :

Output for this phase is top 50 Geospatial hotspots calculated according to Getis-ord statistic algorithm.


INSTRUCTIONS TO RUN JAR

1. Please  use the following command to execute the jar file. 

./bin/spark-submit [spark properties] --class HotspotAnalysis group19_phase3.jar [path to input] [path to output]

2. The output will be saved to the [path to output]/part-000000
