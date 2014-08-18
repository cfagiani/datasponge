datasponge
==========

A simple toolkit for crawling the web and extracting information.

###Overview
The Data Sponge provides a mechanism for performing a targeted crawl of a set of websites rooted at one or more URLs. The tool is configured through a property file (sample below) and uses regular expressions to include/exclude pages.

The processing pipeline for the application is as follows:
* Upon initialization, the system will instantiate the DataExtractor, DataEnhancers and DataWriters configured in the property file.
* The system will build a single Crawler Workqueue and seed it with the start URLs from the configuration file.
* The DataSponge spawns a (configurable) number of SpiderThreads
* Each Spider Thread will consume URLs from the workqueue and fetch the page. 
* All hyperlinks will be extracted from the page and, if they satisfy the inclusion/exclusion rules, they will be added to the workqueue
* The page content will be passed to the Data Extractor and 0 to N DataRecords will be produced
* Each DataRecord will be passed through the DataEnhancer pipeline (if one is configured)
* The DataRecords will then be passed to the DataWriter

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

For all modes except FULLTEXT,  the DataRecord will contain fields that include the index of the match within the body and the context (the match plus a configurable number of characters before/after). For FULLTEXT, the record will contain the entire body of the page.

####Included Writers
* PrintWriter - simply prints the DataRecord to Standard Output. This is primary for debugging.
* CSVFileWriter - writes DataRecords as a CSV file. Based on configuration options, this writer can either append or overwrite files and will include/suppress a header row.

####Included Enhancers
* GroovyEnhancer - a shim that allows for the dynamic loading of a Groovy script

###TODO
* rework programmatic config
* introduce messaging to allow ability to run distributed?


###Sample Property File

    #if no proxy needed, just comment out these two properties
    proxyhost=someproxyserver.yourdomain.com
    proxyport=80

    #comma-delimited list of urls from which to
    starturls=http://www.buffalonews.com

    #regex that urls MUST match to be included in crawl
    includepatterns=.*www.buffalonews.com.*

    #regex of url patterns that will be ignored
    ignorepatterns=.*.rss

    #number of concurrent spider threads.
    maxthreads=10

    #how often the driver checks for completion and flushes output
    sleepinterval=20000


    dataextractor=org.cataractsoftware.datasponge.extractor.HyperlinkExtractor
    datawriter=org.cataractsoftware.datasponge.writer.PrintWriter

    #mode can be "once" or "continuous"
    mode=once
   
    #how often (in seconds) to sleep between continuous crawl invocations
    crawlinterval=600




