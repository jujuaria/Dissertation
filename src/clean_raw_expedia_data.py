import pandas as pd
import os
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

THRESHOLD = 70

file_dir = r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/raw_review_data_2022"

file_name = r"Zhu - Master Sheet and Hotel Information.xlsx"

h_exp = pd.read_excel(os.path.join(file_dir, file_name),"Expedia - Hotel Information")
h_ta = pd.read_excel(os.path.join(file_dir, file_name),"Tripadvisor - Hotel Information")

h_exp = h_exp[~h_exp["Zipcode"].isna()]
h_ta = h_ta[~h_ta["Zipcode"].isna()]
h_exp = h_exp[~h_exp["Expedia URL"].isna()]
h_ta = h_ta[~h_ta["Tripadvisor URL"].isna()]

h_exp = h_exp.drop_duplicates(["Hotel Name","Address"],keep="last")
h_ta = h_ta.drop_duplicates(["Hotel Name","Address"],keep="last")

h_ta["duplicated_address"] =  h_ta["Address"].duplicated(keep=False)
h_ta["duplicated_url"] =  h_ta["Tripadvisor URL"].duplicated(keep=False)

h_exp["duplicated_address"] =  h_exp["Address"].duplicated(keep=False)
h_exp["duplicated_url"] =  h_exp["Expedia URL"].duplicated(keep=False)

h_exp["hotel_name_lower"] = h_exp["Hotel Name"].str.lower()
h_ta["hotel_name_lower"] = h_ta["Hotel Name"].str.lower()
h_exp = h_exp.drop_duplicates(["hotel_name_lower"])
h_ta = h_ta.drop_duplicates(["hotel_name_lower"])

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

h_ta.to_csv(os.path.join(file_dir, r"tripadvisor_hotel_information.csv"),index=False)
h_exp.to_csv(os.path.join(file_dir, r"expedia_hotel_information.csv"),index=False)

h_exp.shape
h_ta.shape

h_exp["Expedia URL"].nunique()
h_exp["Hotel Name"].nunique()
h_exp["Address"].nunique()

h_ta["Tripadvisor URL"].nunique()
h_ta["Hotel Name"].nunique()
h_ta["Address"].nunique()

sum(h_ta["Hotel Name"].isin(h_exp["Hotel Name"]))
sum(h_ta["Address"].isin(h_exp["Address"]))


h_ta = pd.read_csv(os.path.join(file_dir, r"tripadvisor_hotel_information.csv"))
h_exp = pd.read_csv(os.path.join(file_dir, r"expedia_hotel_information.csv"))
'''
locations = h_ta.groupby("Location")

list_name=[]
for name,l in locations:
    h_exp_location = h_exp[h_exp["Location"]==name]
    h_ta_name = list(l["Hotel Name"].unique())
    h_exp_name = list(h_exp_location["Hotel Name"].unique())

    h_ta_l = []
    for hta in h_ta_name:
        ratio_l = []
        for he in h_exp_name:
            ratio_l.append(fuzz.WRatio(hta.lower(), he.lower()))
        df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
        df["Hotel Name - EXP"] = h_exp_name
        df["Hotel Name - TA"] = hta
        df = df.sort_values(by="Ratio",ascending=False)
        h_ta_l.append(df[df["Ratio"]>50])
    data = pd.concat(h_ta_l,axis=0)
    data["Location"] = name
    print("Finished processing -- {}".format(name))
    list_name.append(data)

df_name = pd.concat(list_name)
df_name.to_csv(os.path.join(file_dir,r"hotel_name_match_result.csv"),index= False)

top_match = df_name.groupby("Hotel Name - TA").head(1)
exact_match = top_match[top_match["Ratio"]>=90]
exact_match["duplicated_exp_match"] = exact_match["Hotel Name - EXP"].duplicated(keep=False)
exact_match.to_csv(os.path.join(file_dir,r"hotel_name_exact_match.csv"),index= False)
'''

exact_match = pd.read_csv(os.path.join(file_dir, r"hotel_name_exact_match_result.csv"))
exact_match =exact_match[exact_match["drop_from_exact"]!=1]

exact_match_1 = exact_match.merge(h_ta[["Hotel Name","Tripadvisor URL"]],\
                left_on=["Hotel Name - TA"], right_on=["Hotel Name"])
exact_match_2 = exact_match_1.merge(h_exp[["Hotel Name","Expedia URL"]],\
                 left_on=["Hotel Name - EXP"], right_on=["Hotel Name"])
exact_match_2.to_csv(os.path.join(file_dir,r"hotel_name_exact_match_1.csv"),index= False)

h_ta_2 = h_ta[~h_ta["Tripadvisor URL"].isin(match["Tripadvisor URL"])]
h_exp_2 = h_exp[~h_exp["Expedia URL"].isin(match["Expedia URL"])]

locations = h_ta_2.groupby("Location")

list_name=[]
for name,l in locations:
    h_exp_location = h_exp_2[h_exp_2["Location"]==name]
    h_ta_name = list(l["Hotel Name"].unique())
    h_exp_name = list(h_exp_location["Hotel Name"].unique())

    h_ta_l = []
    for hta in h_ta_name:
        ratio_l = []
        for he in h_exp_name:
            ratio_l.append(fuzz.WRatio(hta.lower(), he.lower()))
        df = pd.DataFrame(ratio_l,columns=["Ratio"],dtype=float)
        df["Hotel Name - EXP"] = h_exp_name
        df["Hotel Name - TA"] = hta
        df = df.sort_values(by="Ratio",ascending=False)
        h_ta_l.append(df[df["Ratio"]>50])
    data = pd.concat(h_ta_l,axis=0)
    data["Location"] = name
    print("Finished processing -- {}".format(name))
    list_name.append(data)

df_name = pd.concat(list_name)
top_match = df_name.groupby("Hotel Name - TA").head(1)
exact_match = top_match[top_match["Ratio"]>=90]
exact_match["duplicated_exp_match"] = exact_match["Hotel Name - EXP"].duplicated(keep=False)
exact_match.to_csv(os.path.join(file_dir,r"hotel_name_exact_match_2.csv"),index= False)

exact_match = pd.read_csv(os.path.join(file_dir, r"hotel_address_exact_match_result.csv"))
exact_match =exact_match[exact_match["exact_match"]==1]

exact_match_1 = exact_match.merge(h_ta_2[["Hotel Name","Address","Tripadvisor URL"]],\
                left_on=["Hotel Address - TA"], right_on=["Address"])

exact_match_2 = exact_match_1.merge(h_exp_2[["Hotel Name","Address","Expedia URL"]],\
                left_on=["Hotel Address - EXP"], right_on=["Address"])

match = pd.concat([name_match[["Expedia URL","Tripadvisor URL"]],\
address_match[["Expedia URL","Tripadvisor URL"]]],axis=0)
h_ta = h_ta.merge(match, on="Tripadvisor URL", how="left")
h_ta.to_csv(os.path.join(file_dir, r"tripadvisor_hotel_information_with_exact_match.csv"), index=False)
