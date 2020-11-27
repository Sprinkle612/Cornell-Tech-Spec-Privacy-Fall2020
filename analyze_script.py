# %%
import numpy as np
import pandas as pd
import sqlite3
import sys
import os

# %%
conn_t = sqlite3.connect("forbes-t1.sqlite")
javascript_cookies_t = pd.read_sql_query(
    "select visit_id, record_type, change_cause, host, name, value, time_stamp from javascript_cookies;", conn_t)
# %%
javascript_cookies_t.groupby('visit_id')['name'].nunique()
# %%
conn_c = sqlite3.connect("forbes-c1.sqlite")
javascript_cookies_c = pd.read_sql_query(
    "select visit_id, record_type, change_cause, host, name, value, time_stamp from javascript_cookies;", conn_c)
# %%
javascript_cookies_c.groupby('visit_id')['name'].nunique()
