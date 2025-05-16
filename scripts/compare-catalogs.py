import requests
import pandas as pd

url1 = "https://catalog-next-dev-datagov.app.cloud.gov/api/action/package_search?q=*:*&facet.field=[%22harvest_source_title%22]&facet.limit=1000"
url2 = "https://catalog.data.gov/api/action/package_search?q=*:*&facet.field=[%22harvest_source_title%22]&facet.limit=1000"

try:
  response1 = requests.get(url1)
  response1.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
  data1 = response1.json()
except requests.exceptions.RequestException as e:
  print(f"Error fetching data from URL1: {e}")
  data1 = {}

try:
  response2 = requests.get(url2)
  response2.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
  data2 = response2.json()
except requests.exceptions.RequestException as e:
  print(f"Error fetching data from URL2: {e}")
  data2 = {}


# Convert JSON data to pandas DataFrames
df1 = pd.json_normalize(data1.get("result").get("facets").get("harvest_source_title"))
df2 = pd.json_normalize(data2.get("result").get("facets").get("harvest_source_title"))

df1 = df1.transpose()
df2 = df2.transpose()

df1 = df1.reset_index().rename(columns={"index": "harvest_source", 0: "catalog_next_counts"})
df2 = df2.reset_index().rename(columns={"index": "harvest_source", 0: "catalog_prod_counts"})

final_output = pd.merge(df1, df2, on="harvest_source", how="outer")

final_output.to_csv("harvest_source_counts.csv", index=False)