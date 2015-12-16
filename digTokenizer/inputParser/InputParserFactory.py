#!/usr/bin/env python

from CSVParser import CSVParser
from JSONParser import JSONParser

class ParserFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_parser(data_type, config, verbose=True, text_format=None, **kwargs):
        parser = None
        if data_type == "csv":
            parser = CSVParser(config, verbose=verbose, **kwargs)
        elif data_type == "json":
            parser = JSONParser(config, verbose=verbose, **kwargs)
        else:
            raise ValueError("Unexpected data_type {}".format(data_type))
        if verbose:
            print("Parser for {} is {}".format(data_type, parser))
        return parser
