#!/usr/bin/env python

import json
import jq
from digSparkUtil.listUtil import flatten

class JSONParser(object):

    def __init__(self, config, **options):
        self.column_paths = list()
        for index in config["fieldConfig"]:
            if "path" in config["fieldConfig"][index]:
                self.column_paths.insert(int(index), config["fieldConfig"][index]["path"])
        # column_paths: now a list of jq-style paths
        self.key_path = 'uri'

    def parse(self, data):
        parsed = self.parse_with_paths(data, self.key_path, self.column_paths)
        #print "\n\n\nReturn:", parsed[0], "value:", parsed[1]
        return (parsed[0], parsed[1])

#     def parse_values(self, data):
#         return self.parse_values_with_paths(data, self.column_paths)

    def parse_values(self, data):
        """Return all values extracted from json data

We have a list of column paths
The path might imply multiple values a la [].description
The data themselves might be stored as a list: "description": [..]

So we could get a triply nested list in general.  I think that would be
too confusing for consumers.

We agreed:
(1) For generality, expect multiple JQ-style paths, so from a given JSON input
one will get a tuple of lists of tokens, one per path.  Note: this is UNLABELED: 
the labels conform to the order given in the config_paths.  This might not 
always be clear.
(2) For any config_path, flatten any multiple values from [] iteration and from 
values being stored as arrays.  I can not think of a usage case where we would want
to distinguish these two cases and would not first transform our own data.

So we get for given path a non-nested list of values.
So we get as full result a tuple of non-nested lists of values.

"""
        # column paths are now ordered
        column_path = self.column_paths[0]
        # temporary list holding values for this datum
        this = []
        for column_path in self.column_paths:
            try:
                values = list(flatten(jq.jq(column_path).transform(data, multiple_output=True)))
            except:
                # if column_path does not apply
                # or any other exception(?)
                # => empty list
                values = []
            this.append(values)
        # coerce to immutable tuple for return value
        return tuple(this)

    def parse_with_key(self, x, key_name):
        return self.parse_values_with_paths(x, key_name, self.column_paths)

    def parse_with_paths(self, json_data, key_name, paths):
        key = json_data[key_name]
        value = self.__extract_columns(json_data, paths)
        return key, value

    def parse_values_with_paths(self, json_data, paths):
        vals = list(self.__extract_columns(json_data, paths))
        return vals

    def __extract_columns(self, row, paths):
        for path in paths:
            x = list(self.__extract_at_path(row, path))
            if (len(x) > 0):
                yield ' '.join(x)

    def __extract_at_path(self, row, path):
        start = self.to_list(row)
        found = True
        path_elems = path.split(".")
        for path_elem in path_elems:
            if not isinstance(start, list):
                break
            start = self.__extract_elements(start, path_elem)
            if len(start) == 0:
                found = False
                break


        #print "FOUND:", found, ":", start
        if found:
            if isinstance(start, list):
                for elem in start:
                   yield elem

            else:
                yield start

    def __extract_elements(self, array, elem_name):
        result = []

        for elem in array:
            if elem_name in elem:
                #print "Elem:", elem, "elem_name:", elem_name
                elem_part = elem[elem_name]
                if isinstance(elem_part, list):
                    result.extend(elem_part)
                else:
                    result.append(elem_part)

        #print "\n\nFind ", elem_name, "in", array, "\nResult:", result
        return result


    def to_list(self, some_object):
        if not isinstance(some_object, list):
            arr = list()
            arr.append(some_object)
            return arr
        return some_object
