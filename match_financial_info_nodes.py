import pickle
import pandas as pd

enron_data = pickle.load(open("final_project_dataset.pkl", "rb"))

attributes = list(enron_data['METTS MARK'].keys())

nodes = pd.DataFrame(enron_data).transpose()

series_once_dropped = nodes.email_address.dropna()
idxs = list(series_once_dropped.index.values)

new_nodes =nodes.loc[idxs]
new_nodes.to_csv('Nodes_with_financial_info.csv')

