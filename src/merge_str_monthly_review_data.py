import pandas as pd
import os

review_folder = r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/monthly_review_data_2022"
str_folder = r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022"

str_file = r"merged_str_ta_update_with_expedia_miami.csv"
exp_file = r"expedia_monthly_ratings.csv"
ta_file = r"tripadvisor_monthly_ratings.csv"

str_data = pd.read_csv(os.path.join(str_folder, str_file))
exp_data = pd.read_csv(os.path.join(review_folder, exp_file))
ta_data = pd.read_csv(os.path.join(review_folder, ta_file))
ta_data = ta_data.rename(columns={"URL":"Tripadvisor URL"})

ta_data = ta_data.drop(['total_one_star_%','total_two_star_%','total_three_star_%','total_four_star_%','total_five_star_%'],axis=1)
exp_data = exp_data.drop(['total_one_star_%','total_two_star_%','total_three_star_%','total_four_star_%','total_five_star_%'],axis=1)

# Drop Miami
str_data = str_data[str_data["City"] != 'Miami']
str_data["Matched Tripadvisor"] = ~str_data["Tripadvisor URL"].isna()
str_data["Matched Expedia"] = ~str_data["Expedia URL"].isna()

str_data["Has Tripadvisor Reviews"] = str_data["Tripadvisor URL"].isin(ta_data["Tripadvisor URL"])
str_data["Has Expedia Reviews"] = str_data["Expedia URL"].isin(exp_data["Expedia URL"])

exp_test = exp_data.merge(str_data[["Expedia URL","STR Number"]], on="Expedia URL")
ta_test = ta_data.merge(str_data[["Tripadvisor URL","STR Number"]], on="Tripadvisor URL")

df_ta = ta_test.drop(["Tripadvisor URL"],axis=1)
df_exp = exp_test.drop(["Expedia URL"],axis=1)
df_str = str_data.drop(["Tripadvisor URL","Expedia URL"],axis=1)

output_dir = r"/Users/juju/Library/Mobile Documents/com~apple~CloudDocs/ta_project/data/STR 2022/send_to_str"
df_str.to_excel(os.path.join(output_dir,r"master_file_without_miami.xlsx"), index=False)
df_ta.to_excel(os.path.join(output_dir,r"tripadvisor_reviews_without_miami.xlsx"), index=False)
df_exp.to_excel(os.path.join(output_dir,r"expedia_reviews_without_miami.xlsx"), index=False)
