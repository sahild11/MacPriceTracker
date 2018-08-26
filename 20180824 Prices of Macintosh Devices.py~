#20180301 - Sahil Dilwali - getting prices of macintosh devices

#import libraries
from lxml import html
import requests
import pandas as pd
import datetime
import csv
#download website contents to file
page = requests.get('https://www.macrumors.com/roundup/best-apple-deals/')
tree = html.fromstring(page.content)
#parse file
products=tree.xpath('//div[@class="product-name"]/text()')
apple   =tree.xpath( '//div[contains(@class,"cell zero")]/a/text()')
amazon  =tree.xpath(  '//div[contains(@class,"cell one")]/a/text()')
adorama =tree.xpath(  '//div[contains(@class,"cell two")]/a/text()')
macmall =tree.xpath(  '//div[contains(@class,"cell three")]/a/text()')
bestbuy =tree.xpath(  '//div[contains(@class,"cell four")]/a/text()')
b_and_h =tree.xpath(  '//div[contains(@class,"cell five")]/a/text()')
#combine into data frame
data=pd.DataFrame({'product': products,'apple': apple,'amazon': amazon, 'adorama': adorama, 'macmall':macmall,'bestbuy':bestbuy,'b_and_h':b_and_h})
#add date to data frame
dt=datetime.datetime.today()
data['date']=dt.strftime("%#m/%#d/%Y")
#Remove commas from apple, amazon, adorama, macmall, bestbuy, b_and_h
cols = ['apple', 'amazon','adorama','macmall','bestbuy','b_and_h']
# pass them to df.replace(), specifying each char and it's replacement:
data[cols] = data[cols].replace({'\$': '', ',': ''}, regex=True)
#print(data)

#add to csv file
import csv
try:
 x=pd.read_csv('my_csv.csv')
 data.to_csv('my_csv.csv', mode='a', header=False, index=False)
except Exception as e:
 data.to_csv('my_csv.csv',mode='w', header=True, index=False)

#de dupe csv file
combined_data=pd.read_csv('my_csv.csv')
print(combined_data.shape)
combined_data.drop_duplicates(subset=['product','date'], keep='first', inplace=True)
print(combined_data.shape)
combined_data.sort_values(['product','date'], ascending=True)
#print(combined_data)
combined_data.to_csv('Final_List.csv', mode='w', header=True, index=False)
