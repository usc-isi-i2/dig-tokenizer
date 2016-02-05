#!/usr/bin/env python

from CSVParser import CSVParser
from JSONParser import JSONParser
from digSparkUtil import logging

class ParserFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_parser(data_type, config, text_format=None, **kwargs):
        parser = None
        if data_type == "csv":
            parser = CSVParser(config, **kwargs)
        elif data_type == "json":
            parser = JSONParser(config, **kwargs)
        else:
            raise ValueError("Unexpected data_type {}".format(data_type))
        logging.info("Parser for {} is {}".format(data_type, parser))
        return parser
