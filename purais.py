#!/usr/bin/env python3
# encoding: utf-8

# SPDX-License-Identifier: GPL-3.0-only

import operator
import os
import sys
import ais
from json.encoder import JSONEncoder
import argparse
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import time
from numbers import Number
from collections import OrderedDict


class LatestCache(OrderedDict):
    def __init__(self, maxsize=1000, maxtime=60):
        super().__init__()
        self.maxsize = maxsize
        self.maxtime = maxtime

    def __setitem__(self, key, value):
        if isinstance(key, Number) and isinstance(value, tuple) and len(value) == 4:
            super().__setitem__(key, value)
            self.move_to_end(key)
        # Pop out items if too many
        if len(self) > self.maxsize:
            self.popitem(0)

    def get_latest(self, wait_secs=60):
        t = time.time()
        keys = list(self.keys())
        ret = []
        if keys[0] < t - wait_secs or len(keys) == self.maxsize:
            t -= self.maxtime
            # Delete too old values first
            [self.pop(k) for k in keys if k < t]
            ret = [self.pop(k) for k in keys if k >= t]
        return ret


def valid_latlon(lat, lon):
    return False if abs(lat) >= 90.0 or abs(lon) >= 180.0 else True


def valid_eta(eta_day, eta_hour, eta_minute):
    try:
        if eta_day is None or eta_hour is None or eta_minute is None:
            return False

        eta_day = int(eta_day)
        eta_hour = int(eta_hour)
        eta_minute = int(eta_minute)
        if (eta_day < 1 or eta_day > 31 or
            eta_hour < 0 or eta_hour > 23 or
            eta_minute < 0 or eta_minute > 59):
            return False
    except:
        return False
    return True


def create_eta(eta_month, eta_day, eta_hour, eta_minute):
    utcnow = datetime.utcnow()
    # Use ongoing month if the eta month is not valid.
    eta_month = eta_month if eta_month is not None and isinstance(eta_month, Number) else utcnow.month
    # This or next year. We assume travelling time under two months.
    eta_year = utcnow.year + 1 if utcnow.month >= 11 and int(eta_month) <= 2 else utcnow.year
    return datetime(eta_year, eta_month, eta_day, eta_hour, eta_minute, 0, tzinfo=timezone.utc).strftime("%b%d %H:%M")


def aivdm_to_jsonais_dict(aivdm, pad, rxtime=datetime.utcnow().strftime("%Y%m%d%H%M%S")):
    decoded = ais.decode(aivdm, pad)
    jdict = {}
    msgtype = decoded.get("id")
    mmsi = decoded.get("mmsi")

    if mmsi is None or msgtype is None:
        return None

    # Common Navigation Block (CNB)
    if msgtype in [1, 2, 3, 18, 19]:
        jdict['msgtype'] = msgtype
        jdict['mmsi'] = mmsi
        jdict['rxtime'] = "{0}".format(rxtime)

        nav_status = decoded.get("nav_status")
        if nav_status is not None:
            jdict['status'] = nav_status

        sog = decoded.get("sog")
        speed = -1 if sog is None else round(float(sog),1)
        jdict['speed'] = speed

        x = decoded.get("x")
        y = decoded.get("y")
        if x is not None and y is not None:
            lon = round(float(x),7)
            lat = round(float(y),7)
            if not valid_latlon(lat, lon):
                return None
            jdict['lon'] = lon
            jdict['lat'] = lat

        cog = decoded.get("cog")
        course = -1 if cog is None else round(float(cog),1)
        jdict['course'] = course

        true_heading = decoded.get("true_heading")
        heading = -1 if true_heading is None else int(true_heading)
        jdict['heading'] = heading

    if msgtype in [5, 24]:
        jdict['msgtype'] = msgtype
        jdict['mmsi'] = mmsi
        jdict['rxtime'] = "{0}".format(rxtime)

        imo_num = decoded.get("imo_num")
        if imo_num is not None:
            jdict['imo'] = imo_num

        draught = decoded.get("draught")
        if draught is not None:
            draught = round(float(draught), 1)
            jdict['draught'] = draught

        destination = decoded.get("destination")
        if destination is not None:
            destination = destination.strip("@").strip(" ")
            jdict['destination'] = "{0}".format(destination)

        eta_month = decoded.get("eta_month")
        eta_day = decoded.get("eta_day")
        eta_hour = decoded.get("eta_hour")
        eta_minute = decoded.get("eta_minute")
        if valid_eta(eta_day, eta_hour, eta_minute):
            jdict['eta'] = create_eta(eta_month, eta_day, eta_hour, eta_minute)

    if msgtype in [27]:

        jdict['msgtype'] = msgtype
        jdict['mmsi'] = mmsi
        jdict['rxtime'] = "{0}".format(rxtime)

        nav_status = decoded.get("nav_status")
        if nav_status is not None:
            jdict['status'] = nav_status

        x = decoded.get("x")
        y = decoded.get("y")
        if x is not None and y is not None:
            lon = round(float(x),7)
            lat = round(float(y),7)
            if not valid_latlon(lat, lon):
                return None
            jdict['lon'] = lon
            jdict['lat'] = lat
        else:
            return None

        sog = decoded.get("sog")
        speed = -1 if sog is None else round(float(sog),1)
        jdict['speed'] = speed

        cog = decoded.get("cog")
        course = -1 if cog is None else round(float(cog),1)
        jdict['course'] = course

        gnss = decoded.get("gnss")
        if gnss is not None:
            jdict['gnss'] = gnss

    if msgtype in [6, 8]:
        dac = decoded.get("dac")
        fi = decoded.get("fi")

        if (msgtype == 6 and dac == 1 and fi == 40) or \
           (msgtype == 8 and dac == 1 and fi == 16) or \
           (msgtype == 8 and dac == 1 and fi == 24):
            persons = decoded.get("persons")
            if persons is None:
                return None
            jdict["persons_on_board"] = persons

        if msgtype == 8 and dac == 200 and fi == 55:
            passengers = decoded.get("passengers")
            crew = decoded.get("crew")
            yet_more_personnel = decoded.get("yet_more_personnel")
            if passengers is None and crew is None and yet_more_personnel is None:
                return None
            persons = 0
            if passengers is not None:
                persons += passengers
            if crew is not None:
                persons += crew
            if yet_more_personnel is not None:
                persons += yet_more_personnel
            if persons == 0:
                return None
            jdict["persons_on_board"] = persons

        if False: # msgtype == 8 and dac == 1 and fi == 31:
            x = decoded.get("x")
            y = decoded.get("y")
            repeat_indicator = decoded.get("repeat_indicator")
            if repeat_indicator > 0:
                return None

            if x is not None and y is not None:
                lon = round(float(x),7)
                lat = round(float(y),7)
                if not valid_latlon(lat, lon):
                    return None
                jdict['lon'] = lon
                jdict['lat'] = lat
            else:
                return None

            wind_ave = decoded.get("wind_ave")
            if wind_ave is not None and wind_ave >= 0 and wind_ave < 127:
                jdict['wind_ave'] = wind_ave

            wind_gust = decoded.get("wind_gust")
            if wind_gust is not None and wind_gust >= 0 and wind_gust < 127:
                jdict['wind_gust'] = wind_gust

            wind_dir = decoded.get("wind_dir")
            if wind_dir is not None and wind_dir >= 0 and wind_dir < 360:
                jdict['wind_dir'] = wind_dir

            air_temp = decoded.get("air_temp")
            if air_temp is not None and air_temp >= -60.0 and air_temp <= 60.0:
                jdict['air_temp'] = round(air_temp, 1)

            rel_humid = decoded.get("rel_humid")
            if rel_humid is not None and rel_humid >= 0 and rel_humid <= 100:
                jdict['rel_humid'] = rel_humid

            air_pres = decoded.get("air_pres")
            if air_pres is not None and air_pres >= 800 and air_pres <= 1200:
                jdict['air_pres'] = air_pres

            horz_vis = decoded.get("horz_vis")
            if horz_vis is not None and horz_vis >= 0 and horz_vis < 127:
                jdict['horz_vis'] = round(horz_vis, 1)

        if len(jdict) > 0:
            jdict['msgtype'] = msgtype
            jdict['mmsi'] = mmsi
            jdict['rxtime'] = "{0}".format(rxtime)
            jdict["dac"] = dac
            jdict["fid"] = fi
        else:
            return None

    if msgtype in [1, 2, 3, 5, 18, 19, 24]:
        name = decoded.get("name")
        if name is not None:
            name = name.strip("@").strip(" ")
            jdict['shipname'] = "{0}".format(name)

        type_and_cargo = decoded.get("type_and_cargo")
        if type_and_cargo is not None:
            jdict['shiptype'] = type_and_cargo

        vendor_id = decoded.get("vendor_id")
        if vendor_id is not None:
            vendor_id = vendor_id.strip("@").strip(" ")
            if vendor_id != "":
                jdict['vendorid'] = "{0}".format(vendor_id)

        callsign = decoded.get("callsign")
        if callsign is not None:
            callsign = callsign.strip("@").strip(" ")
            if len(callsign) > 0:
                jdict['callsign'] = "{0}".format(callsign)

        dim_a = decoded.get("dim_a")
        dim_b = decoded.get("dim_b")
        if dim_a is not None and dim_b is not None:
            jdict['length'] = int(dim_a) + int(dim_b)
            jdict['ref_front'] = int(dim_a)

        dim_c = decoded.get("dim_c")
        dim_d = decoded.get("dim_d")
        if dim_c is not None and dim_d is not None:
            jdict['width'] = int(dim_c) + int(dim_d)
            jdict['ref_left'] = int(dim_c)

    return jdict if len(jdict) > 0 else None


def kvp_filter(val, kvp):
    ok = True
    for item in kvp:
        # e.g. valid kvp item: 'id,eq,1'
        kv_list = item.split(",")
        if len(kv_list) < 3:
            ok = False
            break
        item_key = kv_list[0]
        item_operator = kv_list[1]
        item_values = kv_list[2:]
        value = str(val.get(item_key))

        if item_operator == "eq":
            if value not in item_values:
                ok = False
                break

        elif item_operator == "gt":
            try:
                value = float(value)
                item_value = float(item_values[0])
            except:
                ok = False
                break
            if not operator.gt(value, item_value):
                ok = False
                break

        elif item_operator == "lt":
            try:
                value = float(value)
                item_value = float(item_values[0])
            except:
                ok = False
                break
            if not operator.lt(value, item_value):
                ok = False
                break

        elif item_operator == "le":
            try:
                value = float(value)
                item_value = float(item_values[0])
            except:
                ok = False
                break
            if not operator.le(value, item_value):
                ok = False
                break

        elif item_operator == "ge":
            try:
                value = float(value)
                item_value = float(item_values[0])
            except:
                ok = False
                break
            if not operator.ge(value, item_value):
                ok = False
                break

        elif item_operator == "contains":
            if not operator.contains(str(value), item_values[0]):
                ok = False
                break

    return ok


def to_json_msgs(aivdm_msgs, kvp, latest=False):
    msgs = {}
    i = 0
    val = None
    for item in aivdm_msgs:
        try:
            decoded = ais.decode(item[0], item[1])
        except Exception as e:
            if debug:
                print("FAIL: msg:", item[0], " pad:", item[1],
                      "error_msg:", e, file=sys.stderr)
        if decoded is None:
            continue
        if kvp_filter(decoded, kvp):
            if latest:
                length = len(decoded.keys())
                key = "{0}_{1}_{2}".format(decoded['mmsi'], decoded['id'], length)
            else:
                key = msgs.__len__()
            msgs[key] = decoded

    if len(msgs) > 0:
        return JSONEncoder().encode(list(msgs.values()))
    return None


def to_jsonais(aivdm_msgs, kvp, station_name, latest=False):
    msgs={}
    for item in aivdm_msgs:
        rxtime = datetime.fromtimestamp(item[3], tz=timezone.utc).strftime("%Y%m%d%H%M%S")
        try:
            jsonais_dict = aivdm_to_jsonais_dict(item[0], item[1], rxtime)
        except Exception as e:
            continue
        if jsonais_dict is not None:
            if kvp_filter(jsonais_dict, kvp):
                key = "{0}_{1}_{2}".format(jsonais_dict['mmsi'],
                                           jsonais_dict['msgtype'],
                                           len(jsonais_dict.keys())) if latest else msgs.__len__()
                msgs[key] = jsonais_dict

    if len(msgs) > 0:
        full_json = { 'protocol': 'jsonais',
                      'encodetime': datetime.utcnow().strftime("%Y%m%d%H%M%S"),
                      'groups': [{
                          'path': [{'name': station_name}],
                          'msgs': list(msgs.values())
                      }]
                     }
        return JSONEncoder().encode(full_json)
    return None

def main(args):
    debug = args.debug
    filename = args.filename
    out_format = args.out_format
    hold_secs = min(args.hold_secs, 300) if args.hold_secs > 0  else 0
    channel = args.channel
    station_name = args.station_name
    kvp = args.kvp
    latest = args.latest

    if out_format == "jsonais" and station_name is None:
        raise Exception("Station-name must be given when jsonais output format is used.")

    # Assuming we receive max 20 messages/second
    # Release the cache if maxsize or hold_secs reached.
    # If a message is older than maxtime, drop it.
    maxtime = max(hold_secs, 120)
    maxsize = max(20*maxtime, 1000)
    latest_cache = LatestCache(maxsize=maxsize, maxtime=maxtime)


    ##### Read from a file or from pipe #####

    if filename != "-":
        filename = os.path.expanduser(filename)
        if not os.path.isfile(filename):
            raise Exception(f"File {filename} is not a file")
    else:
        filename = 0
        if sys.stdin.isatty():
            raise Exception("input file missing")

    jemma = []
    first_print = True
    with open(filename, 'r', encoding='utf-8', errors='replace') as f:
        line = True
        while line:
            line = f.readline().replace('\x00', '')

            if not line:
                break

            full_msg = ""
            chan= None
            pad = 0
            try:
                # Don't use invalid messages
                if not ais.nmea.Checksum(line[:-5]) != line[-3]:
                    jemma.clear()
                    continue

                # Use only AIVDM messages
                if line[:6] != "!AIVDM":
                    continue

                # AIVDM message contains 7 fields
                m=line.split(",")
                if len(m) != 7:
                    jemma.clear()
                    continue

                # Print out as is
                if out_format == "raw":
                    if (channel == None or m[4] == channel):
                        print(line, end='')
                    continue

                jemma.append(m)

                # Full message may construct from multiple messages.
                if m[1] != m[2]:
                    continue

                # Construct full message from one or multiple messages.
                for msg in jemma:
                    chan = msg[4]
                    full_msg += msg[5]
                    pad = int(line[-5:-4])

                # Store message
                rxtime = time.time()
                if channel is None or chan == channel:
                    latest_cache[rxtime] = (full_msg, pad, chan, rxtime)

                res = None
                if out_format == "jsonais":
                    # Print messages as jsonais.
                    if station_name is None:
                        raise Exception("Station name not given")
                    aivdm_msgs = latest_cache.get_latest(hold_secs)
                    if len(aivdm_msgs) > 0:
                        res = to_jsonais(aivdm_msgs, kvp, station_name, latest=latest)
                elif out_format == "json":
                    # Print messages as json.
                    aivdm_msgs = latest_cache.get_latest(hold_secs)
                    if len(aivdm_msgs) > 0:
                        res = to_json_msgs(aivdm_msgs, kvp, latest=latest)
                else:
                    raise Exception("Out format failure")

                if res is not None:
                    print(res)

                jemma.clear()

            except Exception as e:
                jemma.clear()
                if debug:
                    print("FAIL: msg:", full_msg.encode(), " pad:", pad, "\t error_msg:", e, file=sys.stderr)


if __name__ == "__main__":
    ret_val = 0
    debug = True
    args = None
    try:
        description = """
Convert AIVDM messages to jsonais

EXAMPLES
    data_source | {0} --out-format raw --channel A
    data_source | {0} --out-format json --hold-secs 5 --kvp repeat_indicator,eq,0
    data_source | {0} --out-format jsonais --station-name testing --latest
    data_source | {0} --out-format jsonais --station-name testing --kvp shiptype,eq,34,36 --channel B --hold-secs 60 --latest
    data_source | {0} --out-format jsonais --station-name testing --kvp lat,gt,59.4 --kvp lat,lt,60.2 --kvp lon,gt,24.4 --kvp lon,lt,25.3 --hold-secs 5 --latest

        """.format(os.path.basename(__file__))

        parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                         description=description)
        parser.add_argument("--filename", default="-", type=str,
                            help="Read messages from a file (Default: '-', from pipe).")
        parser.add_argument("--out-format", choices={"raw","json","jsonais"}, default="raw",
                            help="Print data as raw, json or jsonais format (Default: raw)")
        parser.add_argument("--channel", choices={'A','B'}, default=None,
                            help="Select either 'A' or 'B' messages.")
        parser.add_argument("--station-name", default=None, type=str,
                            help="Station name used with jsonais format.")
        parser.add_argument("--kvp", action="extend", nargs="+", type=str, default=[],
                            help="Filter out messages.")
        parser.add_argument("--latest", action='store_true',
                            help="Print out only latest messages based on mmsi, messge type and number of fields.")
        parser.add_argument("--hold-secs", default=30, type=int,
                            help="Relese messages until the oldest message is n seconds old (default: 30).")
        parser.add_argument("--debug", action='store_true',
                            help="Print errors to stderr")


        args = parser.parse_args()
        debug = args.debug
        main(args)

    except Exception as e:
        ret_val = 1
        if debug:
            print("ERROR:", e, file=sys.stderr)
    except:
        ret_val = 1
        if debug and args is not None:
            print("ERROR: unknown error", file=sys.stderr)

    sys.exit(ret_val)
