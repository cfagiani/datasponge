datasponge
==========

A toolkit for crawling the web and extracting information that can be run on a single host or distributed across multiple hosts (for scalability).
This is a SpringBoot application that can use an embedded ActiveMQ JMS broker (or optionally an external broker). 

###Overview
The Data Sponge provides a mechanism for performing a targeted crawl of a set of websites rooted at one or more URLs. The bulk of the configuration is stored in a JobDefinition JSON document (a sample of which can be found in the test/resources directory).

The processing pipeline for the application is as follows:
* Upon initialization, the system will instantiate the JobController REST API and the JobCoordinator.
* When a job is submitted (either via the command line at startup or via the job submission API), the system will send an announcement message on a JMS topic
* Any nodes connected to the topic that can process the job will respond with an Enrollment message
* After a period of time (10 seconds by default), the coordinator will send ASSIGNMENT messages to all nodes that enrolled. This assignment will specifiy a node ID for each server (used to paritition the set of URLs processed by each node).
* Each enrolled node will construct a crawlerWorkqueue instance that is responsible for holding a queue of URLs to be processed on that node (it consumes incoming URLS from the JMS topic and only adds to the internal queue if the URL is assigned to that node)
* The coordinator will seed the queue with the start urls from the job definition and begin the crawl.
* The actual crawl is performed by a (configurable) number of SpiderThreads
* Each Spider Thread will consume URLs from the workqueue and fetch the page. 
* All hyperlinks will be extracted from the page and, if they satisfy the inclusion/exclusion rules, they will be added to the workqueue
* The page content will be passed to the Data Extractor and 0 to N DataRecords will be produced
* Each DataRecord will be passed through the DataEnhancer pipeline (if one is configured)
* The DataRecords will then be passed to the DataWriter
* If the JMSDataWriter is used, then the job should also specify a "coordinatorDataWriter". That data writer will consume off the ouptut topic and perform the actual data write.

If running in "once" mode, the system will terminate once the work queue is exhausted. If running in "continuous" mode, the system will re-seed the workqueue with the start URLs after a configurable interval.


###Pluggable Components
The system has a pluggable architecture that allows users to easily specify their own DataExtractors, DataEnhancers and DataWriters. In addition to the base classes the define the core functionality, there are a number of components that are included out-of-the-box.

####Included Extractors
* GroovyExtractor - this extractor is a shim that allows for the dynamic loading of a Groovy script.
* HyperlinkExtractor - this extractor will output a single DataRecord per page that has a field entry for every hyperlink on an HTML page.
* TextSearchExtractor - this extractor will parse the page content as text and attempt to find a search string on the page. The search can be case sensitive or insensitive and has 3 modes: 
    * ALL - finds all occurrences of the string on the page (will return a DataRecord per occurrence).
    * FIRST - finds the first occurrence of the string
    * LAST - finds the last occurrence of the string
    * FULLTEXT - determines if the page contains the string
* PdfTextExtractor - this extractor will output a single DataRecord per PDF document (the text will be in a field called "text" within the data record)

For all modes except FULLTEXT,  the DataRecord will contain fields that include the index of the match within the body and the context (the match plus a configurable number of characters before/after). For FULLTEXT, the record will contain the entire body of the page.

####Included Writers
* PrintWriter - simply prints the DataRecord to Standard Output. This is primary for debugging.
* CSVFileWriter - writes DataRecords as a CSV file. Based on configuration options, this writer can either append or overwrite files and will include/suppress a header row.
* JMSDataWriter - publishes DataRecords to a JMS topic

####Included Enhancers
* GroovyEnhancer - a shim that allows for the dynamic loading of a Groovy script

###Potential Enhancements
* base RSS/Atom extractor
* handle different types of binary content (xlsx, etc) for text search
* make into a platform that can handle job submissions - this would include a simple UI for submitting new crawl jobs


###Differences from Version 1.0
Version 2 is significantly different from the initial release. The highlights of the differences are as follows:
* Jobs are now configured via a JSON file
* There is a RESTful API for submitting and querying job status
* Properties can be overridden by creating application.properties in a "config" directory under the directory where the app is installed

