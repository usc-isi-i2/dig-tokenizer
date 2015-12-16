#!/usr/bin/env python

try:
    from pyspark import SparkContext
except:
    print "### NO PYSPARK"
import sys
import argparse
from digTokenizer.tokenizer import Tokenizer
from digSparkUtil.fileUtil import FileUtil
from digSparkUtil.dictUtil import as_dict, dict_minus
from digSparkUtil.miscUtil import seconds_since_epoch

def testTokenizer(sc, input, output, config,
                  limit=None, 
                  sampleSeed=1234,
                  debug=0, 
                  location='hdfs',
                  inputFileFormat="sequence",
                  inputDataType="json",
                  outputFileFormat="sequence",
                  outputDataType="json",
                  verbose=False,
                  **kwargs):

    futil = FileUtil(sc)

    # LOAD DATA
    rdd_ingest = futil.load_file(input, file_format=inputFileFormat, data_type=inputDataType)
    rdd_ingest.setName('rdd_ingest_input')

    # LIMIT/SAMPLE (OPTIONAL)
    if limit==0:
        limit = None
    if limit:
        # Because take/takeSample collects back to master, can create "task too large" condition
        # rdd_ingest = sc.parallelize(rdd_ingest.take(limit))
        # Instead, generate approximately 'limit' rows
        ratio = float(limit) / rdd_ingest.count()
        rdd_ingest = rdd_ingest.sample(False, ratio, seed=sampleSeed)
        
    tokOptions = {"verbose": verbose,
                  "file_format": inputFileFormat,
                  "data_type": inputDataType}
    tokenizer = Tokenizer(config, **tokOptions)
    rdd_tokenized = tokenizer.perform(rdd_ingest)

    rdd_tokenized.saveAsTextFile('/tmp/rdd_tokenized_' + str(seconds_since_epoch()))

    # SAVE DATA
    outOptions = {}
    futil.save_file(rdd_tokenized, output, file_format=outputFileFormat, data_type=outputDataType, **outOptions)

def main(argv=None):
    '''this is called if run from command line'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', required=True)
    parser.add_argument('--inputFileFormat', default='sequence', choices=('text', 'sequence'))
    parser.add_argument('--inputDataType', default='json', choices=('json', 'csv'))

    parser.add_argument('-o','--output', required=True)
    parser.add_argument('--outputFileFormat', default='sequence', choices=('text', 'sequence'))
    parser.add_argument('--outputDataType', default='json', choices=('json', 'csv'))

    parser.add_argument('--config', default=None)

    parser.add_argument('-l','--limit', required=False, default=None, type=int)
    parser.add_argument('-v','--verbose', required=False, help='verbose', action='store_true')

    args=parser.parse_args()
    # Default configuration to empty config
    args.config = args.config or {}

    sparkName = "testTokenizer"
    sc = SparkContext(appName=sparkName)

    kwargs = {"data_type": args.inputDataType,
              "file_format": args.inputFileFormat,
              "limit": args.limit,
              "verbose": args.verbose}

    testTokenizer(sc, args.input, args.output, args.config, **kwargs)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())
