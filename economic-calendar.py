
import datetime
import requests
import bs4
import pandas as pd
import numpy as np
import datetime as dt

import quickstart as qs

import iso8601
import pytz

# Extract
# Scrape website and add data to list
url = 'https://tradingeconomics.com/united-states/calendar'
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}

response = requests.get(url, headers=headers)
bs = bs4.BeautifulSoup(response.text, features='html.parser')
bs_table = bs.find('table', id='calendar')

data = []
date = ""

for row in bs_table.find_all(['thead','tr'], recursive=False):
    time = ""
    description = ""
    country = ""
    level = ""
    
    try:
        if row['class'] == ['table-header']: date = row.th.text.strip()

    except KeyError:
        td = row.find('td')
        time = td.text.strip()
        if td.span['class']: level = td.span['class'][0]
        country_td = td.next_sibling.next_sibling
        country = country_td.text.strip()
        description_td = country_td.next_sibling.next_sibling
        description = description_td.text.strip()
        data.append([date,time,country,level,description])

df = pd.DataFrame(data , columns=['date','time','country','level','summary'])




# Transform - (level, time zone, date/dayofweek)
df['level'] = df.level.str.split('calendar-date-').str[-1]
df['dateYear'] = pd.to_datetime(df['date']).dt.strftime('%Y').astype(int)
df['dateMonth'] = pd.to_datetime(df['date']).dt.strftime('%m').astype(int)
df['dateDay'] = pd.to_datetime(df['date']).dt.strftime('%d').astype(int)

df['miltime'] = df.time
maskAMPM = df['miltime'].str.contains('AM|PM')
df.loc[ maskAMPM, 'miltime'] = df.miltime.str.replace(' ', ':00 ')
df.loc[ maskAMPM, 'miltime'] = pd.to_datetime(df['miltime']).dt.strftime('%H:%M')

df['dateAdded'] = pd.Timestamp("now")
df = df.replace('',np.nan)

print('\n\nEvents Scraped:  ' + str(df['date'].count()))
print(df[['date','time', 'summary']].tail())



# Read CSV file 
file_name = "eco.csv"
df_csv = pd.read_csv(file_name)

df_csv = df_csv.drop_duplicates()
print('\n\nEvents in CSV:  ' + str(df_csv['date'].count()))
print(df_csv[['date','time', 'summary']].tail())




# LOAD

# isolate scraped records, not currently in csv
df_merge = df.merge(df_csv, on=['date','time', 'summary'] ,how='left')
df_merge = df.merge( df_merge.query('dateAdded_y.isnull()'), on=['date','time','summary'], how='inner')
df_merge = df_merge[df.columns]

print('\n\nEvents Scraped but not already in CSV:  ' + str(df_merge['date'].count()))
print(df_merge.tail())

df_merge = pd.concat([df_csv, df_merge],ignore_index=True)
print('\n\nEvents already in CSV:  ' + str(df_merge['date'].count()))
print(df_merge.tail())

newcsvfile = "eco.csv"
df_merge.to_csv(newcsvfile, index=False)
print('\n\nEvents in CSV now:  ' + str(df_merge['date'].count()))


def convertdate(x):
    return iso8601.parse_date(x).astimezone(pytz.utc)


# Query Google Calendar 
newlist = []
try:
    qs.main()
    eventlist = qs.getEventList()

    for event in eventlist:
        summary = event['summary']
        date = event['start']['dateTime']
        #print(date +" " + summary)
        newlist.append([date,summary])
except Exception as e:
        print("yo")
        error = e.args[1]['error']
        if error == "invalid_grant":
             print("YO")
        print(type(e))
        raise
        
df2 = pd.DataFrame(newlist, columns=['date', 'summary'])
df2 = df2.drop_duplicates()
print('\n\nEvents in calendar:  ' + str(df2['date'].count()))

df2['newdate'] = df2['date'].apply(convertdate)
df2['dateYear'] = pd.to_datetime(df2['newdate']).dt.strftime('%Y').astype(int)
df2['dateMonth'] = pd.to_datetime(df2['newdate']).dt.strftime('%m').astype(int)
df2['dateDay'] = pd.to_datetime(df2['newdate']).dt.strftime('%d').astype(int)


# compare csv and calendar
# merge3
df2['incal'] = 'yes'


# query for filtering level 3 + specific events
query_string = 'level==3 or summary.str.contains("Initial jobless claims") or summary.str.contains("GDP Growth Rate") or summary.str.contains("CPI") or summary.str.contains("Core PCE Price Index MoM") or summary.str.contains("New Home Sales MoM")'
df_merge_3 = df_merge.query(query_string, engine='python')
print('\n\nEvents Scraped (filtered):  ' + str(df_merge_3['date'].count()))
print(df_merge_3[['dateYear','dateMonth','dateDay','summary']].tail())



merge4 = df_merge_3.merge(df2, on=['dateYear','dateMonth','dateDay','summary'], how='left')
#print('\n\n\nmerge4 after merge.......')
#print(merge4.columns)
#print(merge4[['dateYear','dateMonth','dateDay', 'summary','level','incal']])
#print('\n\n')

#merge4= merge4.query('incal.isnull()')
merge4= merge4.query('incal.isnull()')[['date_x', 'level','summary','dateYear',
       'dateMonth', 'dateDay', 'miltime']]


print('\n\nEvents Scraped (filtered) not in calendar already:  ' + str(merge4['dateDay'].count()))
print(merge4.tail())

print("yo")

qs.insertEventFromDict(merge4.to_dict('records'))


# print out duplicates (won't do anything since duplicates are already removed above)
#ix = [ x[0] for x in  df2.groupby(list(df2.columns)).groups.values() if len(x) > 1]
#print(df2.reindex(ix))


