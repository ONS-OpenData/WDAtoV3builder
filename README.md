# WDAtoV3builder
Builds experimental V3 input files using codelists and pre-canned CSVs downloaded via the WDA API.

## Requirements

Python 3.

Beautiful Soup 4 (``` pip install bs4```)

Everything else should be standard Python Libraries, specifically: requests, os, zipfile, csv, json, sys

## Usage

Select the ID for the dataset you want to convert from the ONS data explorer (http://web.ons.gov.uk/ons/data/dataset-finder) and use as follows:


```python V3builder.py <IDgoesHere>```


## Caveats

It's built to work perfectly or break, this is deliberate. It will definetly fail on datasets without a public time component. This is fine (we'll get around to them). You should still be able to convert the majority of WDA.

This tool uses line-by-line csv reading rather than dataframes etc, so convert 1 input line into X number of output lines then repeat until EOF.

This decision has multiple consequences, as follows:

1.) You cant run out of RAM. So it will process a 5G csv as easily as a 5meg.

2.) It'll be fairly slow.

3.) We will allready be writing the file before we hit any issues. Therefore the output file is created as 'INCOMPLETE-V3_<filename>' with the 'INCOMPLETE-' prefix only be removed after successful transformation and validation (so in other words, if you've got any files tagged incomplete left over at the end - just DELETE them).

There's minimum housekeeping at the moment so you will end up with un-converted CSVs on your drive. Just be aware that successfully coverted files always start 'V3_', anything else is a temp file and can go.
