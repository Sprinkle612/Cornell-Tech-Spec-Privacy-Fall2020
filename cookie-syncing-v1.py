import numpy as np
import pandas as pd
import sqlite3
import sys
import os
import re
from urllib.parse import urlparse


def get_host_from_headers(x):
    temp = x.replace('"', '').split('],[')
    for t in temp:
        pair = t.replace(']]', '').replace('[[', '').split(',')
        if "Host" in pair:
            host_value = pair[1]
    return host_value


def get_cookies_from_headers(x):
    ls = x.replace('"', '').split('],[')
    for l in ls:
        ta = l.replace(']]', '').split(',')
        if "Cookie" in ta:
            return ta[-1]
    return ''


def extract_host_from_url(url_ls):
    return list(map(lambda x: urlparse(x).netloc.split('.')[1] if len(urlparse(x).netloc.split('.')) > 1 else x, url_ls))


def get_site(site_url):
    return site_url.split(".")[1]


def is_third_party(origin, url_host):
    if origin in url_host:
        return False
    return True


def save_df_to_csv(df, file_name):
    df.to_csv(file_name+'.csv', index=False)


def extract_deep_clean_host(url):
    s = url.strip(".au").strip('.uk')
    return s.split('.')[-2] if len(s.split('.')) > 1 else s


def run_cookie_syncing_analysis(exp_type, exp_group, exp_index):
    sql_file = data_path + exp_type + '-' + \
        exp_group + str(exp_index) + '.sqlite'
    print(sql_file)
    query = """SELECT sv.site_url, sv.visit_id,
            hr.headers, hr.referrer, hr.url, hr.top_level_url
            FROM http_requests as hr LEFT JOIN site_visits as sv
            ON sv.visit_id = hr.visit_id
            """

    conn = sqlite3.connect(sql_file)
    http_requests = pd.read_sql_query(query, conn)

    cookies = pd.read_sql_query(
        "select visit_id, record_type, change_cause, host, name, value from javascript_cookies;", conn)

    # print("*** completed reading files ***")

    http_requests["host"] = list(
        map(lambda x: get_host_from_headers(x), http_requests.headers.tolist()))
    http_requests["cookies"] = list(
        map(lambda x: get_cookies_from_headers(x), http_requests.headers.tolist()))
    http_requests["origin_site"] = list(
        map(lambda x: get_site(x), http_requests.site_url))
    http_requests["is_third_party"] = list(map(lambda a, b: is_third_party(
        a, b), http_requests.origin_site, http_requests.host))

    http_requests_third_party = http_requests[http_requests.is_third_party == True]

    query2 = """SELECT sv.site_url, sv.visit_id,
            jsc.host, jsc.name, jsc.value
            FROM javascript_cookies as jsc LEFT JOIN site_visits as sv
            ON sv.visit_id = jsc.visit_id
            """
    cookies = pd.read_sql_query(query2, conn)

    # print("*** completed running query ***")

    cookies['origin_site'] = list(map(lambda x: get_site(x), cookies.site_url))
    cookies['is_third_party'] = list(
        map(lambda a, b: is_third_party(a, b), cookies.origin_site, cookies.host))

    cookies_first_party = cookies[cookies.is_third_party == False]
    cookies_third_party = cookies[cookies.is_third_party == True]

    cookie_fp_effective = [re.escape(v) for v in set(
        cookies_first_party.value) if len(v) > 5]
    cookie_tp_effective = [re.escape(v) for v in set(
        cookies_third_party.value) if len(v) > 5]
    cookie_effective_all = [re.escape(v)
                            for v in set(cookies.value) if len(v) > 5]

    fp_cookie_values = "|".join(cookie_fp_effective)
    tp_cookie_values = "|".join(cookie_tp_effective)
    all_cookie_values = "|".join(cookie_effective_all)

    http_requests_syncs = http_requests_third_party[http_requests_third_party.url.str.contains(
        all_cookie_values, regex=True)]

    # can tell wether referrer and url are different
    http_requests_syncs['referrer_host'] = list(
        map(lambda x: extract_deep_clean_host(urlparse(x).netloc), http_requests_syncs.referrer))
    http_requests_syncs['is_diff_host'] = list(map(
        lambda a, b: is_third_party(b, a), http_requests_syncs.host, http_requests_syncs.referrer_host))

    # different domain transmitting
    http_requests_syncs = http_requests_syncs[http_requests_syncs.is_diff_host]

    http_requests_syncs['share_fp_cookie'] = http_requests_third_party.url.str.contains(
        fp_cookie_values, regex=True)
    http_requests_syncs['share_tp_cookie'] = http_requests_third_party.url.str.contains(
        tp_cookie_values, regex=True)

    # extract third party
    http_requests_syncs['referred_by_third'] = list(map(lambda a, b: is_third_party(
        a, b), http_requests_syncs.origin_site, http_requests_syncs.referrer_host))
    print("*** printing results for %s***" % sql_file)
    print("-----------------")
    print(http_requests_syncs[http_requests_syncs.share_fp_cookie].shape[0])
    print(http_requests_syncs[http_requests_syncs.share_fp_cookie &
                              http_requests_syncs.referred_by_third].shape[0])
    print(http_requests_syncs[http_requests_syncs.share_tp_cookie].shape[0])
    print(http_requests_syncs[http_requests_syncs.share_tp_cookie &
                              http_requests_syncs.referred_by_third].shape[0])
    print("-----------------")
    print(http_requests_syncs[http_requests_syncs.share_fp_cookie]
          [http_requests_syncs.cookies == ""].shape[0])
    print(http_requests_syncs[http_requests_syncs.share_fp_cookie]
          [http_requests_syncs.cookies == ""][http_requests_syncs.referred_by_third].shape[0])
    print(http_requests_syncs[http_requests_syncs.share_tp_cookie]
          [http_requests_syncs.cookies == ""].shape[0])
    print(http_requests_syncs[http_requests_syncs.share_tp_cookie]
          [http_requests_syncs.cookies == ""][http_requests_syncs.referred_by_third].shape[0])
    print("-----------------")
    # only index 3
    print(http_requests_syncs[http_requests_syncs.visit_id == 3]
          [http_requests_syncs.share_fp_cookie].shape[0])
    print(http_requests_syncs[http_requests_syncs.visit_id == 3]
          [http_requests_syncs.share_fp_cookie][http_requests_syncs.referred_by_third].shape[0])
    print(http_requests_syncs[http_requests_syncs.visit_id == 3]
          [http_requests_syncs.share_tp_cookie].shape[0])
    print(http_requests_syncs[http_requests_syncs.visit_id == 3]
          [http_requests_syncs.share_tp_cookie][http_requests_syncs.referred_by_third].shape[0])
    print("-----------------")
    # only index 3
    print(http_requests_syncs[http_requests_syncs.visit_id == 3]
          [http_requests_syncs.cookies == ""][http_requests_syncs.share_fp_cookie].shape[0])
    print(http_requests_syncs[http_requests_syncs.visit_id == 3][http_requests_syncs.cookies == ""]
          [http_requests_syncs.share_fp_cookie][http_requests_syncs.referred_by_third].shape[0])
    print(http_requests_syncs[http_requests_syncs.visit_id == 3]
          [http_requests_syncs.cookies == ""][http_requests_syncs.share_tp_cookie].shape[0])
    print(http_requests_syncs[http_requests_syncs.visit_id == 3][http_requests_syncs.cookies == ""]
          [http_requests_syncs.share_tp_cookie][http_requests_syncs.referred_by_third].shape[0])
    http_requests_syncs.drop(
        columns=['site_url', 'headers', 'visit_id', 'is_diff_host'], inplace=True)
    http_requests_syncs['exp_type'] = [exp_type] * http_requests_syncs.shape[0]
    http_requests_syncs['exp_group'] = [
        exp_group] * http_requests_syncs.shape[0]
    http_requests_syncs['exp_index'] = [
        exp_index] * http_requests_syncs.shape[0]
    return http_requests_syncs
    print("-----------------")
#     save_df_to_csv(http_requests_syncs, "cookie_syncs_" +file_name)


if __name__ == "__main__":
    data_path = 'exp-data/'
    # file_name = "nyt-c1"
    sites = ["nyt", "forbes", "washington"]
    groups = ["c", "t"]
    big_table = []

    for s in sites:
        for g in groups:
            for i in range(1, 7):
                df = run_cookie_syncing_analysis(s, g, i)
                big_table.append(df)

    result = pd.concat(big_table)
    save_df_to_csv(result, 'cookie_syncs_update')
