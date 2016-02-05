#!/usr/bin/env python

from __future__ import print_function

import sys
from pyspark import SparkContext
from optparse import OptionParser
from digTokenizer.inputParser.InputParserFactory import ParserFactory
from rowTokenizer import RowTokenizer
import json
from digSparkUtil import FileUtil, as_dict, dict_minus, logging

class Tokenizer(object):
    def __init__(self, config_filename, **p_options):
        self.options = as_dict(p_options)
        self.config = FileUtil.get_json_config(config_filename)
        logging.info("Tokenizer {} config with {} using options {}".format(self, self.config, self.options))

    def perform(self, rdd):
        file_format = self.options["file_format"]
        data_type = self.options["data_type"]
        if file_format == "text":
            return self.tokenize_text_rdd(rdd, data_type)
        elif file_format == "sequence":
            return self.tokenize_seq_rdd(rdd, data_type)
        else:
            raise ValueError("Unexpected file_format {}".format(data_type))

    # In: URI => JSON
    # Out: URI => ( [column_path1_stringresult1, column_path1_stringresult2 ..], [column_path2_stringresult1 ..] )
    def tokenize_seq_rdd(self, rdd_input, data_type):
        input_parser = ParserFactory.get_parser(data_type, self.config, **dict_minus(self.options, 'data_type'))
        if input_parser:
            # SEQUENCE: Each RDD element when parsed yields a tuple of strings
            # to be tokenized, and each string yields a list of tokens
            # corresponding to one per input column_path
            rdd_parsed = rdd_input.mapValues(lambda x: input_parser.parse_values(x))
            return self._tokenize_rdd(rdd_parsed)
        else:
            raise ValueError("No input_parser")

    def tokenize_text_rdd(self, rdd_input, data_type):
        input_parser = ParserFactory.get_parser(data_type, self.config, **dict_minus(self.options, 'data_type'))
        if input_parser:
            # TEXT (NEW): Each RDD element when parsed yields a tuple of strings
            # to be tokenized, and each string yields a list of tokens
            # corresponding to one per input column_path
            rdd_parsed = rdd_input.mapValues(lambda x: input_parser.parse_values(x))
            return self._tokenize_rdd(rdd_parsed)
        else:
            raise ValueError("No input parser possible for {}. {}. {}".format(data_type, self.config, self.options))

    def _tokenize_rdd(self, rdd):
        def concatenated_row_tokens(tpl):
            tokens = []
            for s in tpl:
                tokens.extend(self.__get_tokens(s))
            return tokens
        output_rdd = rdd.mapValues(lambda t: concatenated_row_tokens(t))
        return output_rdd

    def __get_tokens(self, row):
        """Row should be a single string"""
        row_tokenizer = RowTokenizer(row, self.config)

        line = row_tokenizer.next()
        while line:
            # print("RETURN line", line[0:100])
            yield line
            line = row_tokenizer.next()


def dump_as_csv(key, values, sep):
    line = str(key)
    for part in values:
        line = line + str(sep) + str(part)
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
    # print "Got options:", c_options
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

