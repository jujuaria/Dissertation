from __future__ import division
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import os


file_dir = r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/review_data_no_text_2022"
for i in [1,2,3,4,5,6,7,8,9,10,11,12]:
  df = pd.read_csv(os.path.join(file_dir,r"Copy of Zhu Tripadvisor Reviews {}.csv".format(i)),\
                   sep=',',header=0,infer_datetime_format=True,parse_dates=['Review Date'])
  df = df[["URL","Review Rating","Review Date","Hotel Response"]]
  l.append(df)

review = pd.concat(l,axis=0)
review["Review Date"] = pd.to_datetime(review["Review Date"])
review["post_month"] = review["Review Date"].dt.to_period('M')

review["Hotel Response"] = review["Hotel Response"].fillna(0)
review["Hotel Response"] = review["Hotel Response"].astype(int)
hotels=review.groupby("URL")



# Avoid divide by Zero

def avoid_zero_denominator_mean(x):
    try:
        y = sum(x)/sum(x!=0)
    except ZeroDivisionError:
        y = 0
    return y

# Aggregate by review post month
l=[]
for name,hotel in hotels:
    aggregations={
    "Review Rating":["mean","count","sum",
               lambda x:sum(x==1),
               lambda x: sum(x==2),
               lambda x: sum(x==3),
               lambda x: sum(x==4),
               lambda x: sum(x==5)
              ],
    "Hotel Response":["sum"]
    }
    d = hotel.groupby("post_month").agg(aggregations)
    d.columns= ["monthly_rating_mean",
               "monthly_review_count",
                "monthly_rating_sum",
               "monthly_one_star_count",
               "monthly_two_star_count",
                "monthly_three_star_count",
                "monthly_four_star_count",
                "monthly_five_star_count",
                "monthly_hotel_response_count"
              ]

    d = d.reset_index()
    d["URL"] = name
    l.append(d)

df=pd.concat(l,axis=0)

#### Some months don't have any reviews
#### Need to correctly fill in the review ratings and review counts for those rows
#### For review count columns, the monthly count needs to be zero
number_of_reviews_cols = [ 'monthly_rating_mean', 'monthly_rating_sum', 'monthly_review_count', 'monthly_one_star_count',
       'monthly_two_star_count', 'monthly_three_star_count', 'monthly_four_star_count', 'monthly_five_star_count','monthly_hotel_response_count']


l=[]

hotels=df.groupby("URL")

for name, h in hotels:
    h_resample = h.set_index('post_month').resample('M').asfreq().ffill().reset_index(level=0, drop=False)
    h_resample["month_with_reviews"] = h_resample["post_month"].isin(h["post_month"])

    for col in number_of_reviews_cols:

        h_resample[col]= np.where(h_resample["month_with_reviews"]==False,
                      0,h_resample[col])
    l.append(h_resample)

df_monthly = pd.concat(l,axis=0)



l=[]
g = df_monthly.groupby("URL")

for name,group in g:
    group["total_rating_sum"] = group["monthly_rating_sum"].expanding().sum()
    group["num_of_reviews"] = group["monthly_review_count"].expanding().sum()
    group["total_one_star"] = group["monthly_one_star_count"].expanding().sum()
    group["total_two_star"] = group["monthly_two_star_count"].expanding().sum()
    group["total_three_star"] = group["monthly_three_star_count"].expanding().sum()
    group["total_four_star"] = group["monthly_four_star_count"].expanding().sum()
    group["total_five_star"] = group["monthly_five_star_count"].expanding().sum()
    group["accum_rating"] = group["total_rating_sum"]/group["num_of_reviews"]
    l.append(group)

df_cum= pd.concat(l,axis=0)

df_cum["total_one_star_%"] = df_cum["total_one_star"]/df_cum["num_of_reviews"]
df_cum["total_two_star_%"] = df_cum["total_two_star"]/df_cum["num_of_reviews"]
df_cum["total_three_star_%"] = df_cum["total_three_star"]/df_cum["num_of_reviews"]
df_cum["total_four_star_%"] = df_cum["total_four_star"]/df_cum["num_of_reviews"]
df_cum["total_five_star_%"] = df_cum["total_five_star"]/df_cum["num_of_reviews"]

df_cum = df_cum.drop(["monthly_rating_sum","total_rating_sum"],axis=1)
# Save result
out_dir = r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/monthly_review_data_2022"

#exact_match = pd.read_csv(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/raw_review_data_2022/tripadvisor_hotel_information_with_exact_match.csv")

#exact_match = exact_match[~exact_match["Expedia URL"].isnull()]

#df_cum[df_cum["URL"].isin(exact_match["Tripadvisor URL"])]["URL"].nunique()

#result = df_cum.merge(exact_match[["Tripadvisor URL","Expedia URL"]], left_on="URL", right_on=["Tripadvisor URL"], how="left")
df_cum.to_csv(os.path.join(out_dir,r"tripadvisor_monthly_ratings.csv"), index = False, encoding='utf-8')
