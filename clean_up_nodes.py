import pandas as pd

# Basically, I found the 100 enron people that were the most active. I only kept those guys as our nodes to avoid having 6000+ nodes
source_file = 'Edges.csv'
df = pd.read_csv(source_file)

nodes_df = pd.DataFrame()


# This is the part that gets the degree of each node
nodes_df['Counter'] = df.Sender.value_counts()[:200]

nodes_df['ID'] = nodes_df['Counter'].keys()

del nodes_df['Counter']

nodes_df['Label'] = ''

for i, address in enumerate(nodes_df['ID']):
    temp = address.split('@')[0]
    name = temp.replace('.', ' ')
    nodes_df['Label'].iloc[i] = name

nodes_df.reset_index(drop=True, inplace=True)

nodes_df.to_csv('Nodes_refined.csv')