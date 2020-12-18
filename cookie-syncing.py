import numpy as np
import pandas as pd
import sqlite3
import sys
import os
import re
from urllib.parse import urlparse

# helper functions
def get_host_from_headers(x):
    temp = x.replace('"','').split('],[')
    for t in temp:
            pair = t.replace(']]','').replace('[[','').split(',')
            if "Host" in pair:
                host_value = pair[1]
    return host_value

def get_cookies_from_headers(x):
    ls = x.replace('"','').split('],[')
    for l in ls:
        ta = l.replace(']]','').split(',')
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


def process(exp_type, exp_group, exp_index):
    data_path = 'exp-data/'
    sql_file = data_path + exp_type + '-' + \
        exp_group + str(exp_index) + '.sqlite'

    ###### http requests ######
    query = """SELECT sv.site_url, sv.visit_id,
        hr.headers, hr.referrer, hr.url
        FROM http_requests as hr LEFT JOIN site_visits as sv
        ON sv.visit_id = hr.visit_id
        """

    conn = sqlite3.connect(sql_file)
    http_requests = pd.read_sql_query(query, conn)

    http_requests["host"] = list(map(lambda x: get_host_from_headers(x), http_requests.headers.tolist()))
    http_requests["cookies"] =  list(map(lambda x: get_cookies_from_headers(x), http_requests.headers.tolist()))
    http_requests["origin_site"] = list(map(lambda x: get_site(x), http_requests.site_url))
    http_requests["is_third_party"] = list(map(lambda a, b: is_third_party(a, b), http_requests.origin_site, http_requests.host))
    http_requests['referrer'] = list(map(lambda x: urlparse(x).netloc, http_requests.referrer))
    http_requests['referred_by_third'] = list(map(lambda a, b: is_third_party(a, b), http_requests.origin_site, http_requests.referrer))

    data = http_requests[http_requests.is_third_party == True][["origin_site", \
                                                            "host", "referrer", "referred_by_third", "url", "cookies"]].reset_index(drop = True)

    ###### cookies ######
    query2 = """SELECT sv.site_url, sv.visit_id,
        jsc.host, jsc.name, jsc.value
        FROM javascript_cookies as jsc LEFT JOIN site_visits as sv
        ON sv.visit_id = jsc.visit_id
        """

    cookies = pd.read_sql_query(query2, conn)
    
    cookies['origin_site'] = list(map(lambda x: get_site(x), cookies.site_url))
    cookies['is_third_party'] = list(map(lambda a, b: is_third_party(a, b), cookies.origin_site, cookies.host))

    cookies_data = cookies[["origin_site", "host", "name", "value", "is_third_party"]]

    cookie_fp = [re.escape(v) for v in set(cookies_data[cookies_data.is_third_party == False].value) if len(v) > 5]
    cookie_tp = [re.escape(v) for v in set(cookies_data[cookies_data.is_third_party == True].value) if len(v) > 5]

    cookie_fp_string = "|".join(cookie_fp)
    cookie_tp_string = "|".join(cookie_tp)

    cookie_values = [re.escape(v) for v in set(cookies.value) if len(v) > 5]
    cookie_values_string = "|".join(cookie_values)


    ###### cookie syncing ######
    cookie_syncs = data[data.url.str.contains(cookie_values_string, regex=True)]
    
    cookie_syncs['share_fp_cookie'] = data.url.str.contains(cookie_fp_string, regex=True)
    cookie_syncs['share_tp_cookie'] = data.url.str.contains(cookie_tp_string, regex=True)

    cookie_syncs['exp_type'] = [exp_type] * cookie_syncs.shape[0]
    cookie_syncs['exp_group'] = [exp_group] * cookie_syncs.shape[0]
    cookie_syncs['exp_index'] = [exp_index] * cookie_syncs.shape[0]

    # results
    cookie_syncs = cookie_syncs[["exp_type", "exp_group", "exp_index", 
        "origin_site", "host", "referrer", "referred_by_third", "url", "cookies",
        "share_fp_cookie", "share_tp_cookie"]].reset_index(drop = True)

    return cookie_syncs


if __name__ == "__main__":
    data_path = 'exp-data/'
    sites = ["nyt", "forbes", "washington"]
    groups = ["c", "t"]
    big_table = []

    for s in sites:
        for g in groups:
            for i in range(1, 7):
                df = process(s, g, i)
                big_table.append(df)
                # save_df_to_csv(df, "cookie_syncs"+s+g+str(i))

                print("%s-%s-%d completed" % (s,g,i))
                print(df.shape[0])

    result = pd.concat(big_table)
    save_df_to_csv(result, 'cookie_syncs_update')



