import pandas as pd


# From the cleaned_up nodes, establish a list of edges which are only emails sent between selected nodes.
edges_file = 'Edges.csv'
nodes_file = 'Nodes.csv'

df_edges = pd.read_csv(edges_file)
df_nodes = pd.read_csv(nodes_file)

new_edges = pd.DataFrame(columns=['Source','Target', 'TimeStamp'])

list_nodes = list(df_nodes['ID'])



count = 0
tot_count = 0
for row in df_edges.itertuples():
    if row.Sender in list_nodes and row.Recipient in list_nodes:
        new_edges.loc[count] = [row.Sender, row.Recipient, row.Timestamp]
        count += 1
    tot_count += 1
    print(count, '/', tot_count)

new_edges.to_csv('Edges_refined.csv')