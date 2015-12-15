#!/usr/bin/env python

from pyspark import SparkContext
from optparse import OptionParser
from digTokenizer.inputParser.InputParserFactory import ParserFactory
from rowTokenizer import RowTokenizer
import json
import urllib
from digSparkUtil.fileUtil import FileUtil
from digSparkUtil.dictUtil import as_dict

class Tokenizer(object):
    def __init__(self, config_filename, p_options):
        self.options = as_dict(p_options)
        self.config = FileUtil.get_json_config(config_filename)

    def tokenize_seq_file(self, spark_context, filename, data_type):
        """deprecated"""
        raw_data = spark_context.sequenceFile(filename)
        input_parser = ParserFactory.get_parser(data_type, self.config, self.options)
        if input_parser:
            data = raw_data.mapValues(lambda x: input_parser.parse_values(x))
            return self.tokenize_rdd(data)

    def tokenize_seq_rdd(self, rdd_input, data_type):
        input_parser = ParserFactory.get_parser(data_type, self.config, self.options)
        if input_parser:
            # Each element when parsed yields a sequence to be tokenized
            rdd_parsed = rdd_input.mapValues(lambda x: input_parser.parse_values(x))
            return self.tokenize_rdd(rdd_parsed)

    def tokenize_text_file(self, spark_context, filename, data_type):
        """deprecated"""
        raw_data = spark_context.textFile(filename)
        input_parser = ParserFactory.get_parser(data_type, self.config, self.options)
        if input_parser:
            data = raw_data.map(lambda x: input_parser.parse(x))
            return self.tokenize_rdd(data)

    def tokenize_text_rdd(self, rdd_input, data_type):
        input_parser = ParserFactory.get_parser(data_type, self.config, self.options)
        if input_parser:
            # Each element when parsed yields a single object to be tokenized
            rdd_parsed = rdd_input.map(lambda x: input_parser.parse(x))
            print(rdd_parsed)
            return self.tokenize_rdd(rdd_parsed)
        else:
            raise ValueError("No input parser possible for {}. {}. {}".format(data_type, self.config, self.options))

    def tokenize_rdd(self, rdd):
        return rdd.flatMapValues(lambda row: self.__get_tokens(row))

    def perform(self, rdd):
        return self.tokenize_rdd(self, rdd)

    def __get_tokens(self, row):
        row_tokenizer = RowTokenizer(row, self.config)

        line = row_tokenizer.next()
        while line:
            #print "RETURN", line[0:100]
            yield line
            line = row_tokenizer.next()


def dump_as_csv(key, values, sep):
    line = key
    for part in values:
        line = line + sep + part
    return line

if __name__ == "__main__":
    """
        Usage: tokenizer.py [input] [config] [output]
    """
    sc = SparkContext(appName="LSH-TOKENIZER")

    usage = "usage: %prog [options] input config output"
    ### TODO: Use argparse and 'choices' to check input
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")
    parser.add_option("-d", "--type", dest="data_type", type="string",
                      help="input data type: csv/json", default="csv")
    parser.add_option("-i", "--inputformat", dest="inputformat", type="string",
                      help="input file format: text/sequence", default="text")
    parser.add_option("-o", "--outputformat", dest="outputformat", type="string",
                      help="output file format: text/sequence", default="text")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    inputFilename = args[0]
    configFilename = args[1]
    outputFilename = args[2]

    tokenizer = Tokenizer(configFilename, c_options)

    fUtil = FileUtil(sc)
    rdd_input = fUtil.load_json_file(inputFilename, c_options.inputformat, {"separator": c_options.separator})

    rdd_input.saveAsSequenceFile('/tmp/abc.def')
    exit(0)

    if c_options.inputformat == "text":
        # rdd = tokenizer.tokenize_text_file(sc, inputFilename, c_options.data_type)
        rdd = tokenizer.tokenize_text_rdd(rdd_input, c_options.data_type)
    elif c_options.inputformat == "sequence":
        # rdd = tokenizer.tokenize_seq_file(rdd, sc, inputFilename, c_options.data_type)
        rdd = tokenizer.tokenize_seq_rdd(rdd_input, c_options.data_type)

    if c_options.outputformat == "text":
        # why does outputformat == text imply dump as CSV.  Why can not dump as JSON
        # Looks like we want output factory
        rdd.map(lambda (key, values): dump_as_csv(key, values, c_options.separator)).saveAsTextFile(outputFilename)
    elif c_options.outputformat == "sequence":
        rdd.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(outputFilename)
