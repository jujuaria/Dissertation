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


biz_url=[]
biz_id=[]
biz_name=[]
biz_rating=[]
biz_review=[]
biz_rank=[]

url_start= "https://www.tripadvisor.com/Hotels-g35805-Chicago_Illinois-Hotels.html"
url=url_start
response=requests.get(url,proxies=proxies,verify=False, allow_redirects=True)
      
soup = bs4.BeautifulSoup(response.content)
#page_list=soup.find(class_="pageNumbers").find_all(class_="pageNum taLnk")
biz_list=soup.find_all(class_="listing")
for biz in biz_list:
      b_url='https://www.tripadvisor.com'+biz.find(class_="property_title").attrs["href"]
      b_id=biz.find(class_="property_title").attrs["id"]
      b_name=biz.find(class_="property_title").contents[0]
      if biz.find(class_="ui_bubble_rating") is None:
          b_rating=0
      else: b_rating= biz.find(class_="ui_bubble_rating")["class"][1]
      if biz.find(class_="review_count") is None:
          b_reviews=0
      else: b_reviews=biz.find(class_="review_count").contents[0]
      if biz.find(class_="popindex") is None:
          b_rank=0
      else: b_rank=biz.find(class_="popindex").contents[0]
      biz_url.append(b_url)
      biz_id.append(b_id)
      biz_name.append(b_name)
      biz_rating.append(b_rating)
      biz_review.append(b_reviews)
      biz_rank.append(b_rank)
for i in range(30,7*30,30):
    url='https://www.tripadvisor.com'+'/Hotels-g35805-oa{}-Chicago_Illinois-Hotels.html'.format(i)
    response=requests.get(url,proxies=proxies,verify=False, allow_redirects=True)
      
    soup = bs4.BeautifulSoup(response.content)
    biz_list=soup.find_all(class_="listing")
    for biz in biz_list:
      b_url='https://www.tripadvisor.com'+biz.find(class_="property_title").attrs["href"]
      b_id=biz.find(class_="property_title").attrs["id"]
      b_name=biz.find(class_="property_title").contents[0]
      if biz.find(class_="ui_bubble_rating") is None:
          b_rating=0
      else: b_rating= biz.find(class_="ui_bubble_rating")["class"][1]
      if biz.find(class_="review_count") is None:
          b_reviews=0
      else: b_reviews=biz.find(class_="review_count").contents[0]
      if biz.find(class_="popindex") is None:
          b_rank=0
      else: b_rank=biz.find(class_="popindex").contents[0]
      biz_url.append(b_url)
      biz_id.append(b_id)
      biz_name.append(b_name)
      biz_rating.append(b_rating)
      biz_review.append(b_reviews)
      biz_rank.append(b_rank)
    

d={"id":biz_id,"name":biz_name,"rating":biz_rating,"number of reviews":biz_review,"rank":biz_rank,"url":biz_url}
df= pd.DataFrame.from_dict(d)

#df.to_csv("/Users/juju/Downloads/chicago_list.csv",encoding='utf8')