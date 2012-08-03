# HadCom.utils
## Overview
This project creates a runnable jar file that can do some common advance functionality with hadoop.

This first version was built for CDH 4.  I will be making it work for CDH 3 shortly.

##Functionality
###[Put](hadcom.utils/wiki/Put-in-detail)
a collections of layered functionality for advance putting.  For details on how to use this functionality [click here](hadcom.utils/wiki/Put-in-detail) The user will be able to use all the following:


Layer 1: Reading
>Delimiter files
>
>Flat Files
>
>Variable Length Delimiter files
>
>Variable Length Flat Files

Layer 2: Aggregating
>Many files into a few
>
>Appending file name to every row of aggregated files

Layer 3: Threading
>Run in single or multi thread mode
>
>Each thread writing to a different HDFS file to increase write speed

Layer 4: Listening
>Report progress to console

Layer 5: Compresing
>Use Snappy, Gzip, or Bzip2

Layer 6: Writing
>Write Sequence Files
>
>Writer Avro Files
>
>Writer Rc Files

###[Route](hadcom.utils/wiki/Route-in-detail)
This allows you to make one or more directories pumps files into HDFS as you favorite splittable formates (sequence, avro, or rc)  Like the "put" functionality the route logic is also layered.

Layer 1: Route
>Event driven
>
>Schedule driven

Layer 2: Put Threads
>Define number of put threads in the thread pool

Layer 3: Put
>Get all the functionality and options from the above put command

###[Get](had com.utils/wiki/Get-in-detail)
hadoop fs -get is good but.  What if you want to get a sequence, avro, or rc file?  And what if you want to be able to read the results?  Well then you can use these get methods to uncompress sequence, avro or rc files into text to your local drive.

###[Out](hadcom.utils/wiki/Out-in-detail)
hadoop fs -text only goes so far this takes us to the next step by being able to output rc files and avro files in clear text.  [Click here](hadcom.utils/wiki/Out-in-detail) for more information.

###Env
>Converting a {key}|{field}|{value} env files to an avro file with a generated schema
>
>Converting a multiple row type file to multiple avro files each having a generated schema