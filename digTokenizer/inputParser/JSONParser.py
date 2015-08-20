#!/usr/bin/env python
import json


class JSONParser:

    def __init__(self, config, options):
        self.column_paths = list()
        for index in config["fieldConfig"]:
            if "path" in config["fieldConfig"][index]:
                self.column_paths.insert(int(index), config["fieldConfig"][index]["path"])

    def parse(self, x):
        return self.parse_with_paths(x, "uri", self.column_paths)

    def parse_with_key(self, x, key_name):
        return self.parse_values_with_paths(x, key_name, self.column_paths)

    def parse_with_paths(self, x, key_name, paths):
        json_data = json.loads(x)
        value = self.__extract_columns(json_data, paths)
        key = json_data[key_name]
        return key, value

    def parse_values(self, x):
        return self.parse_values_with_paths(x, self.column_paths)

    def parse_values_with_paths(self, x, paths):
        json_data = json.loads(x)
        return self.__extract_columns(json_data, paths)

    def __extract_columns(self, row, paths):
        start = self.to_list(row)
        found = True
        for path_elem in paths:
            start = self.__extract_elements(start, path_elem)
            if len(start) == 0:
                found = False
                break

        if found:
            if isinstance(start, list):
                for elem in start:
                    if "uri" in elem:
                        yield elem
            elif "uri" in start:
                yield start

    def __extract_elements(self, array, elem_name):
        result = []
        for elem in array:
            if elem_name in elem:
                elem_part = elem[elem_name]
                if isinstance(elem_part, list):
                    result.extend(elem_part)
                else:
                    result.append(elem_part)
        return result


    def to_list(self, some_object):
        if not isinstance(some_object, list):
            arr = list()
            arr.append(some_object)
            return arr
        return some_object
