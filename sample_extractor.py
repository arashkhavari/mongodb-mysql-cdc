#!/bin/python3                                                                                                                                                                         [154/166]

import json
import dpath.util
import dateutil.parser
import datetime
import time

"""
If exported JSON not hava prefix like "_id/$oid";
If insert from Kafka prefix is "fullDocument/_id/$oid";
If update from Kafka prefix is "updateDescription/updatedFields/_id/$oid";
Date_convert function is change timestamp or TZ time format to iso format;
Delta time for chagne timezone to 'Asia/Tehran';
Boolean_convert function is convert true and false string to True or False;
"""


def date_convert(data):
    var = data
    if var is not None:
        var = datetime.datetime.strptime(var, '%Y-%m-%dT%H:%M:%S.%fZ') + datetime.timedelta(hours=3, minutes=30)
        var = var.strftime("%Y-%m-%d %H:%M:%S")
    else:
        var = None
    return var


def boolean_convert(data):
    var = data
    if var is not None:
        if var == "true":
            var = int(1)
        elif var == "false":
            var = int(0)
    else:
        var = None
    return var


def id(key):
    for x in dpath.util.search(key, "_id/$oid", yielded=True): return x[1]


def date(key):
    for x in dpath.util.search(key, "date/$date", yielded=True): return date_convert(x[1])


def status(key):
    for x in dpath.util.search(key, "status", yielded=True): return x[1]


def name(key):
    for x in dpath.util.search(key, "name", yielded=True): return x[1]


def family(key):
    for x in dpath.util.search(key, "family", yielded=True): return x[1]


def number(key):
    for x in dpath.util.search(key, "number/$numberLong", yielded=True): return x[1]
