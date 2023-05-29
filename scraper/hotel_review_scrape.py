from datetime import date,timedelta as td
import bs4
#import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
#import seaborn as sb
import json
import re
import time
import requests
import sys
import os


proxy_host = "proxy.crawlera.com"
proxy_port = "8010"

proxy_auth = "4575d015dc7b457ab4a5df07a66c05cd:" # API
proxies = {"https": "https://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),
      "http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port)}


def scrape_data(biz):
      ratings=[]
      review_date=[]
      review_quotes=[]
      u_name=[]
      user_location=[]
      user_review_hist=[]
      #user_hotel_review_count=[]
      #user_helpful_review_count=[]
      hotel_response=[]
      url=biz["url"]
      text=[]
      #traved_as=[]
      #value=[]
      #rooms=[]
      #location=[]
      #clean=[]
      #sleep_quality=[]
      #service=[]
      collect_by=[]
      stayed_as=[]
      rating_by_category=[]
      
      dic={'url':url,'ratings':ratings,'review_date':review_date,'review_quotes':review_quotes,
      'user_name':u_name,'user_location':user_location,
      'user_review_hist':user_review_hist,'collect_by':collect_by, 'hotel_response': hotel_response,'review_text':text,'stayed_as':stayed_as,'rating_by_category':rating_by_category}
      

      
      #response=requests.get(url,proxies=proxies,auth=proxy_auth)#verify="c://Users/USER/Downloads/crawlera-ca.crt")
            #response = urllib2.urlopen(url)
      response=requests.get(url,proxies=proxies,verify=False, allow_redirects=True)
      
      soup1 = bs4.BeautifulSoup(response.content)
      
      if soup1.find(class_="review-container") is not None:
      
      
      
          start_page = 'https://www.tripadvisor.com'+soup1.find(class_="review-container").find(class_="quote").find("a",href=True)["href"]
      
      
      # Click on the first review to open pages with full-context reviews
      
      #response=requests.get(start_page,proxies=proxies,auth=proxy_auth)#verify="c://Users/USER/Downloads/crawlera-ca.crt")
            #response = urllib2.urlopen(url)
          response=requests.get(start_page,proxies=proxies,verify=False, allow_redirects=True)
          soup = bs4.BeautifulSoup(response.content)
      
      
      #while (soup.find(class_="nav next disabled") is None)|(soup.find(class_="nav next taLnk ") is not None):
          while soup.find(class_="nav next taLnk ") is not None:

               review_list = soup.find_all(class_="review-container")
               for review in review_list:
                    dic=scrape_review(review,dic)
               page_next='https://www.tripadvisor.com'+soup.find(class_="nav next taLnk ")['href']   
             #response=requests.get(page_next,proxies=proxies,auth=proxy_auth)#verify="c://Users/USER/Downloads/crawlera-ca.crt")
               response=requests.get(page_next,proxies=proxies,verify=False, allow_redirects=True)
      
            #response = urllib2.urlopen(url)
               soup = bs4.BeautifulSoup(response.content)
             
          review_list = soup.find_all(class_="review-container")
             
          for review in review_list:
              dic = scrape_review(review,dic)
          df = pd.DataFrame(dic)
     
          return df


def scrape_review(review,dic):
      if review.find(class_="rating reviewItemInline") is None:
                        rating= None
      else: rating = review.find(class_="rating reviewItemInline").contents[0].attrs['class'][1][-2:]
      
      if  review.find(class_="rating reviewItemInline") is None:
                         date = None
      else: date = review.find(class_="rating reviewItemInline").contents[1].attrs['title']
      
      if  (review.find(class_="noQuotes") is None) or (len(review.find(class_="noQuotes").contents)==0):
                        quotes= None
      else: quotes = review.find(class_="noQuotes").contents[0]
      
      if  review.find(class_="username mo") is None:
                       user_name=None
      else: user_name = review.find(class_="username mo").contents[0].contents[0]
      
      if  review.find(class_="location") is None:
                        user_loc = None
      else:user_loc = review.find(class_="location").contents[0]
      
      if  review.find(class_="badgetext") is None:
                         user_hist=None
      else: user_hist = review.find(class_="badgetext").contents[0]
                 
      if review.find(class_="recommend-titleInline") is None:
                      stayed=None
      else: stayed=review.find(class_="recommend-titleInline").contents[1]
                  
      if review.find(class_="recommend-answer") is None:
                      rating_by_cat = None
      else: rating_by_cat= review.find_all(class_="recommend-answer")
                  #for result in review.find_all(class_="recommend-answer"):
                            #rating_by_cat.append(str(results.find(class_="ui_bubble_rating")["class"][1])+str(results.find(class_="recommend-description").contents[0]))
                  
                  
      if review.find(class_="prw_rup prw_reviews_partner_attribution_hsx") is None:
                      collect=None
      else:collect='partnership'
                  
      if  review.find(class_="mgrRspnInline") is None:
                             hotel_respond=None
      else: hotel_respond = review.find(class_="mgrRspnInline").contents[0].contents[0].contents[0]
       
      if  review.find(class_="partial_entry") is None:
                            review_text=None
      else:  review_text= review.find(class_="partial_entry")
    
      dic["ratings"].append(rating)
      dic["review_date"].append(date)
      dic["review_quotes"].append(quotes)
      dic["user_name"].append(user_name)
      dic["user_location"].append(user_loc)
      dic["user_review_hist"].append(user_hist)
      dic["stayed_as"].append(stayed)
      dic["rating_by_category"].append(rating_by_cat)
      dic["collect_by"].append(collect)
      dic["hotel_response"].append(hotel_respond)
      dic["review_text"].append(review_text)
       
      return dic
       




data=pd.read_csv('~/DATA/miami_hotel_information.csv',sep=',',header=0,encoding = "iso-8859-1")

data=data.astype(str)


for index, row in data.iterrows():
    #try:
       temp=scrape_data(row)
       if temp is not None:
           
           filename='{}_sea.csv'.format(row["id"])
           output_path= os.path.join("~/DATA",filename)
           temp.to_csv(output_path,encoding='utf-8')
       
    #except AttributeError or ValueError:
       #sys.exc_clear()
        
