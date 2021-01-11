import pandas as pd
import re
import tabula
import time

df_rerun = pd.read_csv("./hkjc/overseas_rerun.csv", header = None)
pdf_urls = list(df_rerun[df_rerun.columns[0]])
start_time = time.time()

check_df = []

for i, pdf in enumerate(pdf_urls):
    pdf_url = 'https://racing.hkjc.com' + pdf
    df_pdf = tabula.read_pdf(pdf_url, stream=True)