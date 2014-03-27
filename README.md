datasponge
==========

A simple toolkit for crawling the web and extracting information.

###Some features:
* targeted spidering through inclusion/exclusion expressions
* multi-threaded spidering
* pluggable architecture for custom extractors/writers
* works behind a proxy

###TODO
* rework programmatic config
* support pipeline of enhancers

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





