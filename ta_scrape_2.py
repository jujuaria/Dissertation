from datetime import date,timedelta as td

import bs4
#import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
#import seaborn as sb
import json
import urllib2
#import oauth2
import re
import time
import requests
from requests.auth import HTTPProxyAuth
import sys

proxy_host = "proxy.crawlera.com"
proxy_port = "8010"
proxy_auth = "51a73df0b52e4cba9bf5f7df85bc6dd5:"
proxies = {"https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
      "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}


data=pd.read_csv("/Users/juju/Downloads/chicago_list.csv",sep=',',header=0)
url_list=list(data.url)


name=[]
add=[]
ext_add=[]
zipcode=[]
price_range=[]
hclass=[]
num_rooms=[]
aka=[]
phone=[]
top_amenities=[]
amenities=[]
room_amenities=[]
room_types=[]

def scrape_data(url):
    #response=requests.get(url,proxies=proxies,auth=proxy_auth)
    response=requests.get(url,proxies=proxies,verify=False, allow_redirects=True)
      
    soup = bs4.BeautifulSoup(response.content)
    biz_name=url
    #if ((len(soup.find(class_="street-address").contents)==0)|(soup.find(class_="street-address") is None)):
    if soup.find(class_="street-address") is None:
        biz_add=''
    else: biz_add=soup.find(class_="street-address").contents[0]
    if soup.find(class_="extended-address") is None:
        biz_ext_add=''
    else:
        biz_ext_add=soup.find(class_="extended-address").contents[0]
    if soup.find(class_="locality") is None:
        biz_zip=''
    else: biz_zip=soup.find(class_="locality").contents[0]
    
    if soup.find(class_="list price_range") is None:
        biz_price=''
    else: 
        biz_price=soup.find(class_="list price_range").find_all(class_="item")[1].contents[0]
    if soup.find(class_="prw_rup prw_common_star_rating") is None:
        biz_class=''
    else: 
        biz_class=soup.find(class_="prw_rup prw_common_star_rating").contents[0].get("class")[1]
    if soup.find(class_="list number_of_rooms") is None:
        biz_num_rooms=''
    else: biz_num_rooms=soup.find(class_="list number_of_rooms").contents[1].contents[0]
    
    if soup.find(class_="list aka") is None:
        biz_aka=''
    else: biz_aka = soup.find(class_="list aka").contents[1].contents[0]
    if soup.find(class_="list top_amenities") is None:
        biz_top_amenities=''
    else: biz_top_amenities=[el.text for el in soup.find(class_="list top_amenities").find_all(class_="item")][1:]
    if soup.find(class_="list hotel_amenities") is None:
        biz_amenities=''
    else: biz_amenities=[el.text for el in soup.find(class_="list hotel_amenities").find_all(class_="item")][1:]
    if soup.find(class_="list room_amenities") is None:
        biz_room_amenities=''
    else: biz_room_amenities=[el.text for el in soup.find(class_="list room_amenities").find_all(class_="item")][1:]
    if soup.find(class_="list room_types") is None:
        biz_room_types=''
    else: biz_room_types=[el.text for el in soup.find(class_="list room_types").find_all(class_="item")][1:]
    
    
    name.append(biz_name)
    add.append(biz_add)
    ext_add.append(biz_ext_add)
    zipcode.append(biz_zip)
    price_range.append(biz_price)
    hclass.append(biz_class)
    num_rooms.append(biz_num_rooms)
    aka.append(biz_aka)
    top_amenities.append(biz_top_amenities)
    amenities.append(biz_amenities)
    room_amenities.append(biz_room_amenities)
    room_types.append(biz_room_types)

for url in url_list:
    scrape_data(url)

df=pd.DataFrame({'url':name,'address':add,'ext_address':ext_add,'zipcode':zipcode,
'price range':price_range,'class':hclass,'number of rooms':num_rooms,'AKA':aka,'top amenities':top_amenities,
'amenities':amenities,'room amenities':room_amenities,'room types':room_types})

#hotel["class"]=hotel["class"].str.extract('(\d.\d|\d)')
#hotel_3["price range"]=hotel_3["price_range"].str.extract('(\d-\d)')