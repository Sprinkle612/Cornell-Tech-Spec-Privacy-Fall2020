# %%
import numpy as np
import pandas as pd
import sqlite3
import sys
import os

data_path = "exp-data/"
# %%
conn_t = sqlite3.connect(data_path + "forbes-t3.sqlite")
javascript_cookies_t = pd.read_sql_query(
    "select visit_id, record_type, change_cause, host, name, value, time_stamp from javascript_cookies;", conn_t)
# %%
javascript_cookies_t.groupby('visit_id')['name'].nunique()
# %%
conn_c = sqlite3.connect(data_path + "forbes-c3.sqlite")
javascript_cookies_c = pd.read_sql_query(
    "select visit_id, record_type, change_cause, host, name, value, time_stamp from javascript_cookies;", conn_c)
# %%
javascript_cookies_c.groupby('visit_id')['name'].nunique()

# %%
cookies_t_df = javascript_cookies_t[javascript_cookies_t['visit_id'] == 3]
cookies_t_uniq = cookies_t_df.name.unique()
cookies_c_df = javascript_cookies_c[javascript_cookies_c['visit_id'] == 3]
cookies_c_uniq = cookies_c_df.name.unique()
# %%
# in treatment not in control
set(cookies_t_uniq) - set(cookies_c_uniq)
# %%
# in control not in treatment
set(cookies_c_uniq) - set(cookies_t_uniq)
# %%
