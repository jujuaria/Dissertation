import pandas as pd
import os
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

THRESHOLD = 70

file_dir = r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/raw_review_data_2022"

file_name = r"Zhu - Master Sheet and Hotel Information.xlsx"

h_exp = pd.read_excel(os.path.join(file_dir, file_name),"Expedia - Hotel Information")
h_ta = pd.read_excel(os.path.join(file_dir, file_name),"Tripadvisor - Hotel Information")

str_data = pd.read_excel(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/from_str_before_merge/Select Markets Census Database Miami.xlsx")

#h_ta["Location"] = h_ta["Location"].replace("Washington DC","Washington")
# Expedia daa does not have Boston
#h_exp["Location"] = h_exp["Location"].replace("Washington DC","Washington")
h_ta = h_ta[h_ta["Location"]=="Miami"]
h_exp = h_exp[h_exp["Location"]=="Miami"]
h_ta["Zipcode"] = h_ta["Zipcode"].astype(str)

str_data[str_data["Postal Code"].isin(h_ta["Zipcode"])]

str_data["Address 1"] = str_data["Address 1"].str.lower()
str_data["Address 1"] = str_data["Address 1"].replace("street","st")
str_data["Address 1"] = str_data["Address 1"].replace("boulevard","blvd")
str_data["Address 1"] = str_data["Address 1"].replace("north","n")
str_data["Address 1"] = str_data["Address 1"].replace("south","s")
str_data["Address 1"] = str_data["Address 1"].replace("avenue","ave")

h_ta["Address"] = h_ta["Address"].str.lower()
h_ta["Address"] = h_ta["Address"].replace("street","st")
h_ta["Address"] = h_ta["Address"].replace("boulevard","blvd")
h_ta["Address"] = h_ta["Address"].replace("north","n")
h_ta["Address"] = h_ta["Address"].replace("south","s")
h_ta["Address"] = h_ta["Address"].replace("avenue","ave")


#str_data = str_data[str_data["City"].isin(h_ta["Location"])]
#Drop STR 76670
#str_data = str_data[str_data["STR Number"]!=76670]

h_exp = h_exp[~h_exp["Zipcode"].isna()]
h_ta = h_ta[~h_ta["Zipcode"].isna()]
h_exp = h_exp[~h_exp["Expedia URL"].isna()]
h_ta = h_ta[~h_ta["Tripadvisor URL"].isna()]

h_exp = h_exp.drop_duplicates(["Hotel Name","Address"],keep="last")
h_ta = h_ta.drop_duplicates(["Hotel Name","Address"],keep="last")
str_data = str_data.drop_duplicates(["Hotel Name","Address 1","Address 2"],keep="last")

h_ta["name_address"] = h_ta["Hotel Name"].astype(str)+' '+h_ta["Address"].astype(str)
h_exp["name_address"] = h_exp["Hotel Name"].astype(str)+' '+h_exp["Address"].astype(str)
#str_data["name_address"] = str_data["Hotel Name"].astype(str)+' '+str_data["Address 1"].astype(str) + ' '+ str_data["Address 2"].astype(str)
str_data["name_address"] = str_data["Hotel Name"].astype(str)+' '+str_data["Address 1"].astype(str)

h_exp["hotel_name_lower"] = h_exp["name_address"].str.lower()
h_ta["hotel_name_lower"] = h_ta["name_address"].str.lower()
str_data["hotel_name_lower"] = str_data["name_address"].str.lower()

h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("apartment")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("studio")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("bedroom")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("this is")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("free parking")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("free pkng")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("bnb")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("bed and breakfast")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("bed & breakfast")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("home with")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("size bed")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("sized bed")]
h_exp =h_exp[~h_exp["hotel_name_lower"].str.contains("rental")]

h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("apartment")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("studio")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("bedroom")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("this is")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("free parking")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("free pkng")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("bnb")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("bed and breakfast")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("bed & breakfast")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("home with")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("size bed")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("sized bed")]
h_ta =h_ta[~h_ta["hotel_name_lower"].str.contains("rental")]

#### Merge STR with Tripadvisor
'''
locations = str_data.groupby("City")

list_name=[]
for name,l in locations:
    h_ta_location = h_ta[h_ta["Location"]==name]
    str_name = list(l["hotel_name_lower"].unique())
    h_ta_name = list(h_ta_location["hotel_name_lower"].unique())

    h_l = []
    for h in str_name:
        ratio_l = []
        for hta in h_ta_name:
            ratio_l.append(fuzz.WRatio(h, hta))
        df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
        df["hotel_name TA"] = h_ta_name
        df["hotel_name STR"] = h
        df = df.sort_values(by="Ratio",ascending=False)
        h_l.append(df[df["Ratio"]>50])
    data = pd.concat(h_l,axis=0)
    data["City"] = name
    print("Finished processing -- {}".format(name))
    list_name.append(data)

df_name = pd.concat(list_name)
'''


str_name = list(str_data["hotel_name_lower"].unique())
h_ta_name = list(h_ta["hotel_name_lower"].unique())

h_l = []
for h in str_name:
    ratio_l = []
    for hta in h_ta_name:
        ratio_l.append(fuzz.WRatio(h, hta))
    df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
    df["hotel_name TA"] = h_ta_name
    df["hotel_name STR"] = h
    df = df.sort_values(by="Ratio",ascending=False)
    h_l.append(df[df["Ratio"]>50])
data = pd.concat(h_l,axis=0)


top_match = data.groupby("hotel_name STR").head(1)
exact_match = top_match[top_match["Ratio"]>=90]
exact_match["hotel_name TA"].nunique()

str_data_1 = str_data.merge(exact_match[["hotel_name TA","hotel_name STR"]], left_on="hotel_name_lower",\
                 right_on="hotel_name STR")
merge_1 = str_data_1.merge(h_ta[["Tripadvisor URL","hotel_name_lower"]], left_on="hotel_name TA",\
                           right_on="hotel_name_lower")

str_data["has_exact_match"] = str_data["hotel_name_lower"].isin(exact_match["hotel_name STR"])
h_ta["has_exact_match"] = h_ta["hotel_name_lower"].isin(exact_match["hotel_name TA"])

str_data_2 = str_data[str_data["has_exact_match"]==False]
h_ta_2 = h_ta[h_ta["has_exact_match"]==False]
'''
locations = str_data_2.groupby("City")

list_name=[]
for name,l in locations:
    h_ta_location = h_ta_2[h_ta_2["Location"]==name]
    str_name = list(l["hotel_name_lower"].unique())
    h_ta_name = list(h_ta_location["hotel_name_lower"].unique())

    h_l = []
    for h in str_name:
        ratio_l = []
        for hta in h_ta_name:
            ratio_l.append(fuzz.WRatio(h, hta))
        df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
        df["hotel_name TA"] = h_ta_name
        df["hotel_name STR"] = h
        df = df.sort_values(by="Ratio",ascending=False)
        h_l.append(df[df["Ratio"]>50])
    data = pd.concat(h_l,axis=0)
    data["City"] = name
    print("Finished processing -- {}".format(name))
    list_name.append(data)
'''


str_name = list(str_data_2["hotel_name_lower"].unique())
h_ta_name = list(h_ta_2["hotel_name_lower"].unique())

h_l = []
for h in str_name:
    ratio_l = []
    for hta in h_ta_name:
        ratio_l.append(fuzz.WRatio(h, hta))
    df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
    df["hotel_name TA"] = h_ta_name
    df["hotel_name STR"] = h
    df = df.sort_values(by="Ratio",ascending=False)
    h_l.append(df[df["Ratio"]>50])
data = pd.concat(h_l,axis=0)

top_match = data.groupby("hotel_name STR").head(1)
exact_match_2 = top_match[top_match["Ratio"]>=88]

str_data_2 = str_data_2.merge(exact_match_2[["hotel_name TA","hotel_name STR"]], left_on="hotel_name_lower",\
                 right_on="hotel_name STR")
merge_2 = str_data_2.merge(h_ta_2[["Tripadvisor URL","hotel_name_lower"]], left_on="hotel_name TA",\
                           right_on="hotel_name_lower")

exact_match_2["hotel_name TA"].nunique()

str_data["has_exact_match"] = (str_data["hotel_name_lower"].isin(exact_match["hotel_name STR"]))|\
                               (str_data["hotel_name_lower"].isin(exact_match_2["hotel_name STR"]))
h_ta["has_exact_match"] = (h_ta["hotel_name_lower"].isin(exact_match["hotel_name TA"]))|\
                          (h_ta["hotel_name_lower"].isin(exact_match_2["hotel_name TA"]))

str_data_3 = str_data[str_data["has_exact_match"]==False]
h_ta_3 = h_ta[h_ta["has_exact_match"]==False]

# match on address

str_data_3["address_lower"] = str_data_3["Address 1"].astype(str).str.lower()

h_ta_3["address_lower"] = h_ta_3["Address"].apply(lambda x: x.strip().split(',')[0])
h_ta_3["address_lower"] = h_ta_3["address_lower"].str.lower()

str_data_3["dup"] = str_data_3["address_lower"].duplicated(keep=False)
h_ta_3["dup"] = h_ta_3["address_lower"].duplicated(keep=False)

str_data_4 = str_data_3[str_data_3["dup"]==False]
h_ta_4 = h_ta_3[h_ta_3["dup"]==False]
'''
locations = str_data_4.groupby("City")

list_name=[]
for name,l in locations:
    h_ta_location = h_ta_4[h_ta_4["Location"]==name]
    str_name = list(l["address_lower"].unique())
    h_ta_name = list(h_ta_location["address_lower"].unique())

    h_l = []
    for h in str_name:
        ratio_l = []
        for hta in h_ta_name:
            ratio_l.append(fuzz.WRatio(h, hta))
        df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
        df["hotel_address TA"] = h_ta_name
        df["hotel_address STR"] = h
        df = df.sort_values(by="Ratio",ascending=False)
        h_l.append(df[df["Ratio"]>50])
    data = pd.concat(h_l,axis=0)
    data["City"] = name
    print("Finished processing -- {}".format(name))
    list_name.append(data)

df_name = pd.concat(list_name)
'''

str_name = list(str_data_4["address_lower"].unique())
h_ta_name = list(h_ta_4["address_lower"].unique())

h_l = []
for h in str_name:
    ratio_l = []
    for hta in h_ta_name:
        ratio_l.append(fuzz.WRatio(h, hta))
    df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
    df["hotel_address TA"] = h_ta_name
    df["hotel_address STR"] = h
    df = df.sort_values(by="Ratio",ascending=False)
    h_l.append(df[df["Ratio"]>50])
data = pd.concat(h_l,axis=0)

top_match = data.groupby("hotel_address STR").head(1)
exact_match_3 = top_match[top_match["Ratio"]==100]
exact_match_3["hotel_address TA"].nunique()

str_data_4 = str_data_4.merge(exact_match_3[["hotel_address TA","hotel_address STR"]], left_on="address_lower",\
                 right_on="hotel_address STR")
merge_3 = str_data_4.merge(h_ta_4[["Tripadvisor URL","address_lower"]], left_on="hotel_address TA",\
                           right_on="address_lower")

exact_merge = pd.concat([merge_1[["STR Number","Tripadvisor URL"]],\
                         merge_2[["STR Number","Tripadvisor URL"]],\
                         merge_3[["STR Number","Tripadvisor URL"]]], axis=0)

str_data_4 = str_data[~str_data["STR Number"].isin(exact_merge["STR Number"])]
h_ta_4 = h_ta[~h_ta["Tripadvisor URL"].isin(exact_merge["Tripadvisor URL"])]

str_data_4["name_lower"] = str_data_4["Hotel Name"].str.lower()
h_ta_4["name_lower"] = h_ta_4["Hotel Name"].str.lower()

str_data_4["dup"] = str_data_4["name_lower"].duplicated(keep=False)
h_ta_4["dup"] = h_ta_4["name_lower"].duplicated(keep=False)

str_data_5 = str_data_4[str_data_4["dup"]==False]
h_ta_5 = h_ta_4[h_ta_4["dup"]==False]
'''
locations = str_data_5.groupby("City")

list_name=[]
for name,l in locations:
    h_ta_location = h_ta_5[h_ta_5["Location"]==name]
    str_name = list(l["name_lower"].unique())
    h_ta_name = list(h_ta_location["name_lower"].unique())

    h_l = []
    for h in str_name:
        ratio_l = []
        for hta in h_ta_name:
            ratio_l.append(fuzz.WRatio(h, hta))
        df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
        df["hotel_name TA"] = h_ta_name
        df["hotel_name STR"] = h
        df = df.sort_values(by="Ratio",ascending=False)
        h_l.append(df[df["Ratio"]>50])
    data = pd.concat(h_l,axis=0)
    data["City"] = name
    print("Finished processing -- {}".format(name))
    list_name.append(data)

df_name = pd.concat(list_name)
'''

str_name = list(str_data_5["name_lower"].unique())
h_ta_name = list(h_ta_5["name_lower"].unique())

h_l = []
for h in str_name:
    ratio_l = []
    for hta in h_ta_name:
        ratio_l.append(fuzz.WRatio(h, hta))
    df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
    df["hotel_name TA"] = h_ta_name
    df["hotel_name STR"] = h
    df = df.sort_values(by="Ratio",ascending=False)
    h_l.append(df[df["Ratio"]>50])
data = pd.concat(h_l,axis=0)

top_match = data.groupby("hotel_name STR").head(1)
exact_match = top_match[top_match["Ratio"]>=90]
exact_match["hotel_name TA"].nunique()

str_data_5 = str_data_5.merge(exact_match[["hotel_name TA","hotel_name STR"]], left_on="name_lower",\
                 right_on="hotel_name STR")
merge_4 = str_data_5.merge(h_ta_5[["Tripadvisor URL","name_lower"]], left_on="hotel_name TA",\
                           right_on="name_lower")

exact_merge = pd.concat([merge_1[["STR Number","Tripadvisor URL"]],\
                         merge_2[["STR Number","Tripadvisor URL"]],\
                         merge_3[["STR Number","Tripadvisor URL"]],\
                          merge_4[["STR Number","Tripadvisor URL"]]],axis=0)
exact_merge["duplicated_ta"] = exact_merge["Tripadvisor URL"].duplicated(keep=False)

str_data_4 = str_data[~str_data["STR Number"].isin(exact_merge["STR Number"])]
h_ta_4 = h_ta[~h_ta["Tripadvisor URL"].isin(exact_merge["Tripadvisor URL"])]

#temp = str_data.merge(exact_merge, on="STR Number", how="left")

#temp.to_csv(r"/Users/juju/Downloads/df_name.csv")
str_data_4["Telephone"] = str_data_4["Telephone"].astype(str)
h_ta_4["Phonenumber"]=h_ta_4["Phonenumber"].astype(str)
'''
locations = str_data_4.groupby("City")

list_name=[]
for name,l in locations:
    h_ta_location = h_ta_4[h_ta_4["Location"]==name]
    str_name = list(l["Telephone"].unique())
    h_ta_name = list(h_ta_location["Phonenumber"].unique())

    h_l = []
    for h in str_name:
        ratio_l = []
        for hta in h_ta_name:
            ratio_l.append(fuzz.WRatio(h, hta))
        df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
        df["hotel_name TA"] = h_ta_name
        df["hotel_name STR"] = h
        df = df.sort_values(by="Ratio",ascending=False)
        h_l.append(df[df["Ratio"]>50])
    data = pd.concat(h_l,axis=0)
    data["City"] = name
    print("Finished processing -- {}".format(name))
    list_name.append(data)

df_name = pd.concat(list_name)
'''
str_name = list(str_data_4["Telephone"].unique())
h_ta_name = list(h_ta_4["Phonenumber"].unique())

h_l = []
for h in str_name:
    ratio_l = []
    for hta in h_ta_name:
        ratio_l.append(fuzz.WRatio(h, hta))
    df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
    df["hotel_name TA"] = h_ta_name
    df["hotel_name STR"] = h
    df = df.sort_values(by="Ratio",ascending=False)
    h_l.append(df[df["Ratio"]>50])
data = pd.concat(h_l,axis=0)

top_match = data.groupby("hotel_name STR").head(1)
exact_match_4 = top_match[top_match["Ratio"]>=90]
exact_match_4=exact_match_4[exact_match_4["hotel_name STR"]!='nan']

str_data_4 = str_data_4.merge(exact_match_4[["hotel_name TA","hotel_name STR"]], left_on="Telephone",\
                 right_on="hotel_name STR")
merge_5 = str_data_4.merge(h_ta_4[["Tripadvisor URL","Phonenumber"]], left_on="hotel_name TA",\
                           right_on="Phonenumber")

exact_merge = pd.concat([merge_1[["STR Number","Tripadvisor URL"]],\
                         merge_2[["STR Number","Tripadvisor URL"]],\
                         merge_3[["STR Number","Tripadvisor URL"]],\
                          merge_4[["STR Number","Tripadvisor URL"]],\
                         merge_5[["STR Number","Tripadvisor URL"]]],axis=0)
exact_merge["duplicated_ta"] = exact_merge["Tripadvisor URL"].duplicated(keep=False)
temp = str_data.merge(exact_merge, on="STR Number", how="left")
temp.to_csv(r"/Users/juju/Downloads/df_name.csv")
#########################################################################
str_data = pd.read_excel(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/from_str_before_merge/Select Markets Census Database.xlsx")
matched = pd.read_csv(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/merged_str_ta.csv")
file_dir = r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/raw_review_data_2022"

str_data = str_data[str_data["City"].isin(h_ta["Location"])]
#Drop STR 76670
str_data = str_data[str_data["STR Number"]!=76670]

file_name = r"Zhu - Master Sheet and Hotel Information.xlsx"
h_ta = pd.read_excel(os.path.join(file_dir, file_name),"Tripadvisor - Hotel Information")

not_matched = str_data[~str_data["STR Number"].isin(matched["STR Number"])]
h_ta = h_ta[~h_ta["Tripadvisor URL"].isin(matched["Tripadvisor URL"])]

h_ta["Location"] = h_ta["Location"].replace("Washington DC","Washington")

h_ta = h_ta[~h_ta["Zipcode"].isna()]
h_ta = h_ta[~h_ta["Tripadvisor URL"].isna()]

h_ta_4 = h_ta.drop_duplicates(["Hotel Name","Address"],keep="last")
#str_data_4 = str_data.drop_duplicates(["Hotel Name","Address 1","Address 2"],keep="last")


not_matched["name_lower"] = not_matched["Hotel Name"].str.lower()
h_ta_4["name_lower"] = h_ta_4["Hotel Name"].str.lower()

h_ta_4["name_lower"] = h_ta_4["name_lower"].replace("st.","st")
#need manual match W Boston and W Miami

not_matched["dup"] = not_matched["name_lower"].duplicated(keep=False)
h_ta_4["dup"] = h_ta_4["name_lower"].duplicated(keep=False)

str_data_5 = not_matched[not_matched["dup"]==False]
h_ta_5 = h_ta_4[h_ta_4["dup"]==False]

locations = str_data_5.groupby("City")

list_name=[]
for name,l in locations:
    h_ta_location = h_ta_5[h_ta_5["Location"]==name]
    str_name = list(l["name_lower"].unique())
    h_ta_name = list(h_ta_location["name_lower"].unique())

    h_l = []
    for h in str_name:
        ratio_l = []
        for hta in h_ta_name:
            ratio_l.append(fuzz.WRatio(h, hta))
        df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
        df["hotel_name TA"] = h_ta_name
        df["hotel_name STR"] = h
        df = df.sort_values(by="Ratio",ascending=False)
        h_l.append(df[df["Ratio"]>50])
    data = pd.concat(h_l,axis=0)
    data["City"] = name
    print("Finished processing -- {}".format(name))
    list_name.append(data)

df_name = pd.concat(list_name)
top_match = df_name.groupby("hotel_name STR").head(1)
exact_match_1 = top_match[top_match["Ratio"]>=90]

not_matched = not_matched.merge(exact_match_1[["hotel_name TA","hotel_name STR"]], left_on="name_lower",\
                 right_on="hotel_name STR")
merge_1 = not_matched.merge(h_ta_4[["Tripadvisor URL","name_lower"]], left_on="hotel_name TA",\
                           right_on="name_lower")
merge_1 = merge_1.drop_duplicates(subset=["Tripadvisor URL"])


matched_df = pd.concat([matched[["STR Number","Tripadvisor URL"]],\
                    merge_1[["STR Number","Tripadvisor URL"]]],axis=0)

result = str_data.merge(matched_df, on="STR Number", how="left")
result_matched = result[~result["Tripadvisor URL"].isna()]

total_hotels = result.groupby("City")["STR Number"].nunique().reset_index()
matched_hotels = result_matched.groupby("City")["STR Number"].nunique().reset_index()
match_rate = matched_hotels.merge(total_hotels, on="City")

result.to_csv(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/merged_str_ta_update.csv", index=False)
match_rate.to_csv(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/match_rate.csv", index=False)

#############################################################################
#### Match with Expedia
#df = pd.read_csv(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/merged_str_ta_update.csv")
df = pd.read_csv(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/merged_str_ta_miami.csv")
exp = pd.read_csv(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/raw_review_data_2022/tripadvisor_hotel_information_with_exact_match.csv")
exp = exp[~exp["Expedia URL"].isna()]

data = df.merge(exp[["Tripadvisor URL","Expedia URL"]], on="Tripadvisor URL", how="left")
result_matched = data[~data["Expedia URL"].isna()]
data[~data["Tripadvisor URL"].isna()]
total_hotels = data.groupby("City")["STR Number"].nunique().reset_index()
matched_hotels = result_matched.groupby("City")["STR Number"].nunique().reset_index()
match_rate = matched_hotels.merge(total_hotels, on="City")
match_rate.to_csv(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/match_rate_with_expedia.csv", index=False)

#data.to_csv(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/merged_str_ta_update_with_expedia.csv", index=False)
data.to_csv(r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/merged_str_ta_update_with_expedia_miami.csv", index=False)
