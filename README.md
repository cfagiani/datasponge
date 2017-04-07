datasponge
==========

A toolkit for crawling the web and extracting information that can be run on a single host or distributed across multiple hosts (for scalability).
This is a Spring Boot application that can use an embedded ActiveMQ JMS broker (or optionally an external broker). 

### Overview
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

If running in "once" mode, the job executor will terminate once the work queue is exhausted. If running in "continuous" mode, the system will re-seed the workqueue with the start URLs after a configurable interval. 
In either case, though, the entire system remains running since additional jobs can be submitted via the REST API. 


### Pluggable Components
The system has a pluggable architecture that allows users to easily specify their own DataExtractors, DataEnhancers and DataWriters. In addition to the base classes the define the core functionality, there are a number of components that are included out-of-the-box.

#### Included Extractors
* GroovyExtractor - this extractor is a shim that allows for the dynamic loading of a Groovy script.
* HyperlinkExtractor - this extractor will output a single DataRecord per page that has a field entry for every hyperlink on an HTML page.
* TextSearchExtractor - this extractor will parse the page content as text and attempt to find a search string on the page. The search can be case sensitive or insensitive and has 3 modes: 
    * ALL - finds all occurrences of the string on the page (will return a DataRecord per occurrence).
    * FIRST - finds the first occurrence of the string
    * LAST - finds the last occurrence of the string
    * FULLTEXT - determines if the page contains the string
* PdfTextExtractor - this extractor will output a single DataRecord per PDF document (the text will be in a field called "text" within the data record)
* DirectoryExtractor - crawls a local file system recursively and emits DataRecords for each file 

For all modes except FULLTEXT,  the DataRecord will contain fields that include the index of the match within the body and the context (the match plus a configurable number of characters before/after). For FULLTEXT, the record will contain the entire body of the page.

#### Included Writers
* PrintWriter - simply prints the DataRecord to Standard Output. This is primary for debugging.
* CsvFileWriter - writes DataRecords as a CSV file. Based on configuration options, this writer can either append or overwrite files and will include/suppress a header row.
* JMSDataWriter - publishes DataRecords to a JMS topic
* JdbcDataWriter - allows for insertion of data into a JDBC datasource. This is an abstract writer where subclasses can supply whatever mapping from DataRecords to db records they desire.

#### Included Enhancers
* GroovyEnhancer - a shim that allows for the dynamic loading of a Groovy script
* DeduplicationEnhancer - attempts to detect and remove duplicate data records (local to a single node)

### Prerequisites
* JRE 1.7 or higher

### Configuration
#### Application Properties
The following properties can be set in a application.properties file:
* proxyhost - http proxy to use when fetching remote pages (defaults to none)
* proxyport - port to use for the proxy (defaults to 80; ignored if no proxyhost is set)
* jms.broker.url - connection string to connect to message broker (defaults to vm://datasponge?create=false)

#### Job Config File
The job configuration file is a well-formed JSON document described by the schema document located in the "doc" directory


### Failures
If a node fails, so long as it is not the coordinator for that job, the remaining nodes will be notified and will split the responsibility for processing the data that was destined for the failed node. It is possible
that some pages within the crawl (anything accepted by the node prior to failure but not yet processed) will not be crawled.

If the coordinator fails, the job may be contining to run on the other nodes in the ensemble. If this is the case, their local output will still be written but
any data destined for the executor on the coordinator will not be processed. Similarly, if the job was running with a coordinatorDataWriter set, this will not be running.

As of now, once a node is failed, it stays failed. There is no facility to re-join a job that is in progress.


### Differences from Version 1.0
Version 2 is significantly different from the initial release. The highlights of the differences are as follows:
* Jobs are now configured via a JSON file
* There is a RESTful API for submitting and querying job status
* Properties can be overridden by creating application.properties in a "config" directory under the directory where the app is installed
* The job can be run on an ensemble of hosts by running multiple instances of datasponge connecting to a single message broker

### Potential Enhancements and TODOs
* better error handling for the REST API including validation
* stand-alone job validation tool (schema validation?)
* base RSS/Atom extractor
* handle different types of binary content (xlsx, etc) for text search
* UI for submitting/monitoring crawl jobs
* more tests
* ability to load class files(for custom DataAdapters) from external jars/locations
* ability to join (or re-join) a job in progress
* ability to recover from coordinator failures
* pluggable mechanism to track "seen pages" to handle massive crawls
* pluggable mechanism to fetch pages in SpiderThread (thus allowing non-web/filesystem extraction jobs)
* piggyback heartbeats on other messages and only send HB if needed
