import sqlite3
import pandas as pd

#Connect to SQLite database

conn = sqlite3.connect("churches.db")

# Fetch all churches
query = "SELECT name, website, address FROM churches"
df = pd.read_sql(query, conn)


#Save to CSV for labeling 
df.to_csv("mlchurches_to_label.csv", index=False)

print("Houston churches exported and ready for labeling.")