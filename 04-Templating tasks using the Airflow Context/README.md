We will work out several components of operators with the help of a fictitious stock market prediction tool that applies sentiment analysis, which we will call StockSense. We will apply the axiom that an increase in a company's pageviews shows a positive sentiment, and the company's stock is likely to increase and viceversa.
The Wikimedia Foundation (the organization behind Wikipedia) provides all pageviews
since 2015 in machine-readable format.1 The pageviews can be downloaded in gzip
format and are aggregated per hour per page. Each hourly dump is approximately
50 MB in gzipped text files and is somewhere between 200 and 250 MB in size
unzipped.
<br>
Let’s download one single hourly dump and inspect the data by hand. In order to
develop a data pipeline, we must understand how to load it in an incremental fashion
and how to work the data
<br>

We will create the first version of a DAG pulling in the wikipedia pageview counts. Let us start by downloading, extracting and reading the data. We've selected 5 companies (Amazon, Apple, Facebook, Google, Microsoft) to initially track and validate the hypothesis. The first step is to download the .zip file for every interval. For every interval, we'll have to insert the date and time for that specific interval in the URL