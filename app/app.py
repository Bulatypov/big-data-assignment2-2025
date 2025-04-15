from cassandra.cluster import Cluster
print("App entry")

# Connecting to the cassandra server
cluster = Cluster(['cassandra-server'])

# Creating session
session = cluster.connect()

# Show keyspaces
table = session.execute('DESC keyspaces')

for line in table:
    print(line)
