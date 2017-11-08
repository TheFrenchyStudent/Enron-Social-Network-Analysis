import pandas as pd
import pickle

# Matches the email addresses for whom we have financial info with all of the edges, which yields our final edges file.

enron_data = pickle.load(open("final_project_dataset.pkl", "rb"))
emails = []
for indiv in enron_data:
    emails.append(enron_data[indiv]['email_address'])


df_edges = pd.read_csv('Edges.csv')

new_edges = pd.DataFrame(columns=['Source','Target', 'TimeStamp'])

count = 0
tot_count = 0
for row in df_edges.itertuples():
    if row.Sender in emails and row.Recipient in emails:
        new_edges.loc[count] = [row.Sender, row.Recipient, row.Timestamp]
        count += 1
    tot_count += 1
    print(count, '/', tot_count)

new_edges.to_csv('Edges_refined_with_financial_info.csv')