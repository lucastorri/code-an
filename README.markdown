Git/Jira Code Analysis
======================


Main SBT Commands
-----------------

> ./sbt compile  # compile the source code
> ./sbt run      # run the project's main class
> ./sbt clean    # clean all compiled files, etc
> ./sbt console  # open a REPL with all the project libraries loaded


Git Log
-------

File generated using:
> git log --numstat -w --pretty=format:'=====%n%H;%an;%at;%f'


Creating your own Analyzer
----------------------

All you have to do is create a class that extends *com.thoughtworks.dod.Analyzer*


Useful Links
------------

* [Scala API](http://www.scala-lang.org/api)
* [Spark RDD Documentation](http://www.spark-project.org/docs/0.6.0/api/core/index.html#spark.RDD)
