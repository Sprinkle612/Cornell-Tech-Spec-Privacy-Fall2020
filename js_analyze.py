import pandas as pd
import numpy as np
import sqlite3
from urllib.parse import urlparse
from adblockparser import AdblockRules
from util import get_base_script_url, get_site, extract_host_from_url, is_third_party, save_df_to_csv
from collections import defaultdict

EXP_PATH = "exp-data/"

CANVAS_READ_FUNCS = [
    "HTMLCanvasElement.toDataURL",
    "CanvasRenderingContext2D.getImageData"
]

CANVAS_WRITE_FUNCS = [
    "CanvasRenderingContext2D.fillText",
    "CanvasRenderingContext2D.strokeText"
]

CANVAS_FP_DO_NOT_CALL_LIST = ["CanvasRenderingContext2D.save",
                              "CanvasRenderingContext2D.restore",
                              "HTMLCanvasElement.addEventListener"]

WEBRTC_FP_CALLS = ["RTCPeerConnection.createDataChannel",
                   "RTCPeerConnection.createOffer"]

# identify advertising trackers
easylist_filename = "adblock/easylist.txt"
# identify non-advertising trackers
easyprivacy_filename = "adblock/easyprivacy.txt"

IMAGE_MIN_HEIGHT = 16
IMAGE_MIN_WIDTH = 16

MIN_CANVAS_TEXT_LEN = 10
COOKIE_READ_IN_JS = "window.document.cookie"

INSIGNIFICANT_COOKIE = 'Test|test|lang|SameSite|Samesite|sameSite|samesite|expires|Path|path|domain|Expires|max-age'
INSIGNIFICANT_COOKIE_LS = INSIGNIFICANT_COOKIE.split('|')


def image_is_too_small(arguments):
    if arguments is None:
        return True
    if safe_int(arguments[2]) < IMAGE_MIN_WIDTH or safe_int(arguments[3]) < IMAGE_MIN_HEIGHT:
        return True


def read_ab_rules_from_file(filename):
    filter_list = set()
    for l in open(filename):
        if len(l) == 0 or l[0] == '!':  # ignore these lines
            continue
        else:
            filter_list.add(l.strip())
    return filter_list


def safe_int(string):
    """Convert string to int, return 0 if conversion fails."""
    try:
        return int(string)
    except ValueError:
        return 0


def split_cookie_value(value):
    return set(map(lambda x: (x.split('=')[0].strip(), x.split('=')[1].strip()) if len(x.split('=')) > 1 else (x, None), value.split(';')))


def get_base_script_url(script_url):
    script_url_no_param = script_url.split("?")[0].split("&")[0].split("#")[0]
    return script_url_no_param.split("://")[-1].strip()


def read_rules_from_file(filename):
    filter_list = set()
    for l in open(filename):
        if len(l) == 0 or l[0] == '!':  # ignore these lines
            continue
        else:
            filter_list.add(l.strip())
    return filter_list


def make_cookie_into_df(cookie_dict):
    first_party_ls = []
    script_host_ls = []
    script_url_ls = []
    cookie_name_ls = []
    cookie_value_ls = []
    for first_party, value_set in cookie_dict.items():
        first_party_ls.extend([first_party] * len(value_set))
        for script_host, script_url, cookie_name, cookie_value in value_set:
            script_host_ls.append(script_host)
            script_url_ls.append(script_url)
            cookie_name_ls.append(cookie_name)
            cookie_value_ls.append(cookie_value)
    return pd.DataFrame.from_dict({'origin_site': first_party_ls, 'script_host': script_host_ls, 'script_url': script_url_ls,
                                   'cookie_name': cookie_name_ls, 'cookie_value': cookie_value_ls})


def drop_cookie_from_df(df):
    df.drop(df[df.script_host.apply(lambda x: len(x) < 5 and x != 'g')].index,
            inplace=True)
    df.drop(df[df.cookie_value.apply(lambda x: len(x) < 5)].index, inplace=True)


def process_cookie_from_js(third_party_js_script):
    third_party_js_script_cookie_operations = third_party_js_script[third_party_js_script['symbol'].str.contains(
        "window.document.cookie")]

    # origin_host: (script_host, script_url, cookie_name, cookie_value)
    third_party_set_cookie = defaultdict(set)
    third_party_get_cookie = defaultdict(set)

    for index, row in third_party_js_script_cookie_operations.iterrows():
        if row['value'] is None:
            continue
        if row["operation"] == "set":
            for cookie_name, cookie_value in split_cookie_value(row["value"]):
                if cookie_value == None or cookie_name in INSIGNIFICANT_COOKIE_LS:
                    continue
                third_party_set_cookie[row['first_party_host']].add(
                    (row['script_host'], row['script_url'], cookie_name, cookie_value))
        elif row["operation"] == "get":
            for cookie_name, cookie_value in split_cookie_value(row["value"]):
                if cookie_value == None or cookie_name in INSIGNIFICANT_COOKIE_LS:
                    continue
                third_party_get_cookie[row['first_party_host']].add(
                    (row['script_host'], row['script_url'], cookie_name, cookie_value))

    get_cookie_js = make_cookie_into_df(third_party_get_cookie)
    set_cookie_js = make_cookie_into_df(third_party_set_cookie)
    drop_cookie_from_df(get_cookie_js)
    drop_cookie_from_df(set_cookie_js)
    return get_cookie_js, set_cookie_js


def extract_third_party_js(file_name):
    query = """SELECT sv.site_url, sv.visit_id,
        js.script_url, js.operation, js.arguments, js.symbol, js.value
        FROM javascript as js LEFT JOIN site_visits as sv
        ON sv.visit_id = js.visit_id WHERE
        js.script_url <> ''
        """

    conn = sqlite3.connect(EXP_PATH+file_name)
    js = pd.read_sql_query(query, conn)

    js["first_party_host"] = list(map(lambda x: get_site(x), js["site_url"]))
    js["script_host"] = extract_host_from_url(js.script_url)
    js['is_third_party'] = is_third_party(js.first_party_host, js.script_host)
    third_party_js = js[js['is_third_party'] == True]

    return third_party_js


def convert_text(arguments):
    if not arguments:
        return ""
    return arguments[0].encode('ascii', 'ignore')


MIN_FONT_FP_FONT_COUNT = 50


def canvas_font_fingerprinters(canvas_fonts, canvas_measure_text_calls):
    canvas_font_fingerprinting = defaultdict(set)
    for first_party_url, script_dict in canvas_fonts.items():
        for script_base_url, fonts_value in script_dict.items():
            if len(fonts_value) < MIN_FONT_FP_FONT_COUNT:
                continue
            for measure_url, args, call_count in canvas_measure_text_calls[first_party_url].items():
                if call_count > MIN_FONT_FP_FONT_COUNT and measure_url == script_base_url:
                    canvas_font_fingerprinting.add(
                        (first_party_url, script_base_url))

    return canvas_font_fingerprinting


def get_webrtc_fingerprinters(webrtc_calls):
    webrtc_fingerprinters = defaultdict(set)
    for first_host, script_dict in webrtc_calls.items():
        for script_url, sym_set in script_dict.items():
            if len(sym_set) == 3:
                webrtc_fingerprinters[first_host].add(script_url)
    return webrtc_fingerprinters


def get_canvas_features(third_party_js_script):
    canvas_reads = defaultdict(set)
    canvas_text = defaultdict(lambda: defaultdict(set))
    canvas_write = defaultdict(lambda: defaultdict(set))
    canvas_styles = defaultdict(lambda: defaultdict(set))
    canvas_banned_calls = defaultdict(set)
    canvas_fonts = defaultdict(lambda: defaultdict(set))
    canvas_measure_text_calls = defaultdict(lambda: defaultdict(int))
    webrtc_calls = defaultdict(lambda: defaultdict(set))
    for index, row in third_party_js_script.iterrows():
        script_host = row.script_host
        base_script_url = get_base_script_url(row.script_url)
        if row.symbol in CANVAS_READ_FUNCS and row.operation == "call":
            if row.symbol == "CanvasRenderingContext2D.getImageData" and image_is_too_small(row['arguments']):
                continue
            canvas_reads[row.first_party_host].add(
                (row.script_host, base_script_url))
        elif row.symbol in CANVAS_WRITE_FUNCS:
            text = convert_text(row.arguments)
            if len(text) >= MIN_CANVAS_TEXT_LEN:
                canvas_write[row.first_party_host][base_script_url].add(text)
        elif row.symbol == "CanvasRenderingContext2D.fillStyle" and row.operation == "set":
            if row.value != None:
                canvas_styles[row.first_party_host][base_script_url].add(
                    row.value)
        elif row.operation == "call" and row.symbol in CANVAS_FP_DO_NOT_CALL_LIST:
            canvas_banned_calls[row.first_party_host].add(
                (script_host, base_script_url))
        elif row.symbol == "CanvasRenderingContext2D.font" and row.operation == "set":
            canvas_fonts[row.first_party_host][base_script_url].add(row.value)
        elif row.symbol == "CanvasRenderingContext2D.measureText" and row.operation == "call":
            canvas_measure_text_calls[row.first_party_host][(
                base_script_url, row.arguments)] += 1
        elif (row.operation == "call" and row.symbol in WEBRTC_FP_CALLS) or\
            (row.operation == "set" and
             row.symbol == "RTCPeerConnection.onicecandidate"):
            webrtc_calls[row.first_party_host][base_script_url].add(row.symbol)

    return canvas_font_fingerprinters(canvas_fonts, canvas_measure_text_calls), get_webrtc_fingerprinters(webrtc_calls)


easylist = read_rules_from_file(easylist_filename)
easylist_rule = AdblockRules(easylist)

easyprivacy = read_rules_from_file(easyprivacy_filename)
easyprivacy_rule = AdblockRules(easyprivacy)


def define_easyrule_trackers(script_url, is_third_party):
    easyprivacy_set = set()
    easylist_set = set()
    for url in script_url:
        script_url = get_base_script_url(url)
        if easylist_rule.should_block(script_url, {'script': True,
                                                   'third-party': is_third_party}):
            easylist_set.add(script_url)
        if easyprivacy_rule.should_block(script_url, {'script': True,
                                                      'third-party': is_third_party}):
            easyprivacy_set.add(script_url)

    return easylist_set, easyprivacy_set


if __name__ == "__main__":

    file_name = "forbes-c1"
    sql_name = file_name + '.sqlite'
    print('----read third party js----')
    third_party_js = extract_third_party_js(sql_name)
    easylist_set, easyprivacy_set = define_easyrule_trackers(
        third_party_js.script_url, True)
    print(easylist_set)
    # print('----extract cookie info----')
    # get_cookie_js, set_cookie_js = process_cookie_from_js(third_party_js)
    # print('----save cookie----')
    # save_df_to_csv(get_cookie_js, 'get_cookies_' + file_name)
    # save_df_to_csv(set_cookie_js, 'set_cookies_' + file_name)
