#!/usr/bin/env python

from CSVParser import CSVParser
from JSONParser import JSONParser

class ParserFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_parser(name, config, verbose=True, textFormat=None, **kwargs):
        parser = None
        if textFormat == "csv":
            parser = CSVParser(config, verbose=verbose, **kwargs)
        elif textFormat == "json":
            parser = JSONParser(config, verbose=verbose, **kwargs)
        if verbose:
            print("Parser for {} is {}".format(textFormat, parser))
        return parser
