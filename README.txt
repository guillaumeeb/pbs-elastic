This repo contains part of the configuration used on CNES cluster to index PBS logs and cluster occupancy metrics.
Please be tolerant, this has been done on free time, and the shared files are not totally clean and well described.

Two main pipelines are used for PBS stats, and are described below.

## Index pbs accounting logs

Files are in the accounting-logs subfolder.
This pipeline is a classic ELK pipeline:
1. Filebeat harvest the pbs accounting log files and continuously send new lines to Logstash. This is what is in the filebeat configuration file. Really simple.
2. Logstash cut and organize the data to index it correctly into Elasticsearch. This is the most important part, and shared into the logstash filter file.
3. Index must be declared into Kibana, and then a lot of views can be created. I do not share this part, because I think it is up to you to create those. Kibana is really simple to use. Some examples of views that are useful to us:
  * Just figures : number of submitted jobs, successful jobs, errored jobs...
  * Histogram: the number of jobs submitted accross time, split by users.
  * Histogram: the number of jobs ended accross time.
  * Pie chart: user submittting the most jobs, user consuming the most cpu time...
  * Table with user consumsion information
  * Other informaton split by queue or walltime or project or anything!

## Cluster occupancy: periodic qstat and pbsnodes

File is in the realtime-load subfolder.
I this case, this is just a Python script which is called every 5 minutes through a CRON on a machine with qstat and pbsnodes command access.  
Output of both commands are parsed using Pandas, cleaned and fed directly into Elasticsearch.  
This can be used to plot some nice things like:
* Cluster real occupancy accross time.
* Cluster occupancy split by user, by queue, by any resources.
* Jobs queued accross time: is there a lot waiting to be processed?
