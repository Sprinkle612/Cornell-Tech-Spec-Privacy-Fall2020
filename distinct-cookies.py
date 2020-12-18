import numpy as np
import pandas as pd
import sqlite3
import sys
import os

###### helper functions ######

def clean_host(host):
    return host.split('.')[-2]

def get_site(site_url):
    return site_url.split(".")[1]

def is_third_party(origin, cookie_host):
    if origin in cookie_host:
        return False
    return True

def save_df_to_csv(df, file_name):
    df.to_csv(file_name+'.csv', index=False)

def process(exp_type, exp_group, exp_index):
    data_path = 'exp-data/'
    sql_file = data_path + exp_type + '-' + \
        exp_group + str(exp_index) + '.sqlite'

    ###### javascript cookies  ######
    query = """SELECT sv.site_url, sv.visit_id,
        jsc.host, jsc.name, jsc.value
        FROM javascript_cookies as jsc LEFT JOIN site_visits as sv
        ON sv.visit_id = jsc.visit_id
        """

    conn = sqlite3.connect(sql_file)

    cookies = pd.read_sql_query(query, conn)

    cookies['origin_site'] = list(map(lambda x: get_site(x), cookies.site_url))
    cookies["host"] = list(map(lambda x: clean_host(x), cookies.host.tolist()))
    cookies["is_third_party"] = list(map(lambda a, b: is_third_party(a, b), cookies.origin_site, cookies.host))

    cookies['exp_type'] = [exp_type] * cookies.shape[0]
    cookies['exp_group'] = [exp_group] * cookies.shape[0]
    cookies['exp_index'] = [exp_index] * cookies.shape[0]

    cookies = cookies[["exp_type", "exp_group", "exp_index", "visit_id", "origin_site", "host", "name", "value", "is_third_party"]]
    cookies = cookies.drop_duplicates().reset_index(drop = True)

    return cookies


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
    save_df_to_csv(result, 'distinct_cookies_update')



