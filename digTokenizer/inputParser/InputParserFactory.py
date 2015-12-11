#!/usr/bin/env python

from CSVParser import CSVParser
from JSONParser import JSONParser

class ParserFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_parser(name, config, options, verbose=True):
        parser = None
        if name == "csv":
            parser = CSVParser(config, options)
        elif name == "json":
            parser = JSONParser(config, options)
        if verbose:
            print("Parser for {} is {}".format(name, parser))
        return parser
