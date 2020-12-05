import pandas as pd
import numpy as np
import sqlite3
from urllib.parse import urlparse
from adblockparser import AdblockRules


def get_base_script_url(script_url):
    script_url_no_param = script_url.split("?")[0].split("&")[0].split("#")[0]
    return script_url_no_param.split("://")[-1].strip()


def get_site(site_url):
    return site_url.split(".")[1]


def extract_host_from_url(url_ls):
    return list(map(lambda x: urlparse(x).netloc.split('.')[1] if len(urlparse(x).netloc.split('.')) > 1 else x, url_ls))


def is_third_party(primary_host, secondary_host):
    result = []
    for ph, sh in zip(primary_host, secondary_host):
        if ph == sh:
            result.append(False)
        else:
            result.append(True)
    return result


def save_df_to_csv(df, file_name):
    df.to_csv(file_name+'.csv', index=False)

# if easylist_rules.should_block(script_url, {'script': True,
#                              'third-party': third_party_script}):
#     asylist_blocked_scripts.add(script_host))

# if (symbol == "CanvasRenderingContext2D.getImageData" and
#                     is_get_image_data_dimensions_too_small(arguments)):
#                 continue
