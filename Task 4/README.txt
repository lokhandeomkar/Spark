Impression-click rate is # of clicks divided by # of impressions (records with the "impression" field equal to 1)
A list of minute-of-the-days (integers in [0, 1440)) ordered by impression-click rate in descending order-- only for minutes with nonzero rates is the output.

--------------------

Running Instructions
1. Go to the folder where Spark is installed 
2. Use the following command

./bin/spark-submit --class mp21 --master local[*] [path to the Task1 simple-project-1.0.jar] [path to cs511_data file] [path to output folder in Task1 folder]
./bin/spark-submit --class mp22 --master local[*] [path to the Task2 simple-project-1.0.jar] [path to cs511_data file] [path to output folder in Task2 folder]
./bin/spark-submit --class mp23 --master local[*] [path to the Task3 simple-project-1.0.jar] [path to cs511_data file] [path to output folder in Task3 folder]
./bin/spark-submit --class mp24 --master local[*] [path to the Task4 simple-project-1.0.jar] [path to cs511_data file] [path to output folder in Task4 folder]

There may or may not be a need to 

The output file will be generated in the output folder. It is important that there be an output folder for tasks 1,3,4 and there not be an output folder for task. I have the folders set up correctly as required.

The running time is < 10s for every task. 

The hierarchy for the .java file:

In any [Task *] folder, src->main->java.


NOTE: There may be changes needed to the pom.xml file in each folder.
spark-core, use scope "provided"
hadoop-client, use scope "provided"
