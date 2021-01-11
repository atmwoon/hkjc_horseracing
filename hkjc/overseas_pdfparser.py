import pandas as pd
import re
import tabula
import time

df_horseinfo = pd.read_csv('./hkjc-horseinfo.csv') #directory name
pdf_urls = list(df_horseinfo[~df_horseinfo['overseas_url'].isnull()]['overseas_url'].unique())
start_time = time.time()

for i, pdf in enumerate(pdf_urls):
    pdf_url = 'https://racing.hkjc.com' + pdf
    df_pdf = tabula.read_pdf(pdf_url, stream=True)
    try:
        df_pdf[df_pdf.columns[1]] = df_pdf[df_pdf.columns[[1,2]]].apply(lambda x: ''.join(x.dropna().astype(str)).strip(), axis=1)
        df_pdf = df_pdf.drop(df_pdf.columns[2], axis=1)
        df_pdf = df_pdf.drop(df_pdf.columns[[i for i, val in enumerate(df_pdf.loc[0]) if i>2 and str(val) == 'nan']],axis=1)

        df_pdf.columns = ['date_venue','course_type', 'distance', 'going', 'race_name', 'group', 'position', 'draw', 'weight_carried', 'odds', 'gear',
                    'margin','jockey', 'race_time', 'race_value', 'prize']

        df_parse = df_pdf.iloc[1:df_pdf.index[df_pdf['date_venue']=="RACE RECORD SUMMARY"][0],:]

        agg_d = {'date_venue': lambda x: '|'.join(x.dropna()), 'course_type':'first', 'distance':'first', 'going':'first', 'race_name':'first', 'group':'first',
                'position':'first', 'draw':'first', 'weight_carried':'first', 'odds':'first', 'gear':'first', 'margin':'first', 'jockey':'first', 
                'race_time':lambda x: '|'.join(x.dropna()),'race_value':'first', 'prize':lambda x: '|'.join(x.dropna())}
        df_parse = df_parse.groupby(df_parse.race_name.notnull().cumsum().rename(None)).agg(agg_d)
        
        df_parse['date'] = df_parse['date_venue'].apply(lambda x: x[re.search(r'\|',x).span()[1]:].strip())
        df_parse['venue'] = df_parse['date_venue'].apply(lambda x: x[4:re.search(r'\|',x).span()[0]].strip())
        df_parse['prize_hkd'] = df_parse['prize'].apply(lambda x: int(x[re.search(r'\(HKD.*?\)',x).span()[0]+5: \
                                                                        re.search(r'\((HKD.*?)\)', x).span()[1]-1].replace(',','')) \
                                                                        if re.search(r'\(HKD.*?\)',x) else 0)
        df_parse['prize'] = df_parse['prize'].apply(lambda x: x[0:re.search(r'\ \d{1}\D{2}\:',x).span()[0]])
        df_parse['finish_time'] = df_parse['race_time'].apply(lambda x: x[re.search(r'\(.*?\)',x).span()[0]+1:re.search(r'\(.*?\)',x).span()[1]-1])
        df_parse['race_time'] = df_parse['race_time'].apply(lambda x: x[0:re.search(r'\|',x).span()[0]].strip())
        df_parse['overseas_url'] = pdf

        df_parse = df_parse[['date', 'venue', 'course_type', 'distance' , 'going', 'race_name', 'group', 'position', 'draw', 'weight_carried', 'odds', 'gear',
                    'margin','jockey', 'race_time', 'finish_time', 'race_value', 'prize', 'prize_hkd', 'overseas_url']]
        
        if i == 0:
            df_merge = df_parse
        else:
            df_merge = pd.concat([df_merge, df_parse])

        elapsed_time = time.time() - start_time
        print("{0:.2f}% Complete, Elapsed Time = {1}".format((i+1)/len(pdf_urls)*100,elapsed_time))
    except:
        print("########### Failed for {0}".format(pdf_url))

df_merge.to_csv('../hkjc-overseas.csv', index=False)