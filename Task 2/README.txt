This is a theta join task which is equivalent to the following SQL query 

SELECT a.ymdh, a.user_id, b.user_id
FROM   data a, data b
WHERE  a.click = 1 AND b.click = 1
AND    a.user_id != null AND b.user_id != null
AND    a.user_id < b.user_id
AND    abs(TIMESTAMPDIFF(SECOND, a.ymdh, b.ymdh)) < 2;

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
