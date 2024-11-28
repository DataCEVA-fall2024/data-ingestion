import os
import requests
import csv

# Overpass API endpoint
url = "http://overpass-api.de/api/interpreter"

# Overpass QL query for neighborhoods in Wisconsin
query = """
[out:json][timeout:180];
area["name"="United States"]->.searchArea;
(
  node["place"="neighbourhood"](area.searchArea);
);
out center;
"""

# Fetch data from Overpass API
response = requests.get(url, params={'data': query})
data = response.json()

# Parse the data into a list of dictionaries
neighborhoods = []
for element in data["elements"]:
    node = {
        "name": element['tags'].get('name', 'Unnamed'),
        "region_type": element['tags'].get('place', 'Unknown'),
        "lat": element['lat'],
        "lon": element['lon']
    }
    neighborhoods.append(node)

# Determine the user's Downloads folder
downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
csv_file_path = os.path.join(downloads_folder, "neighborhoods_geo_data.csv")

# Write to a CSV file
with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=["name", "region_type", "lat", "lon"])
    writer.writeheader()
    writer.writerows(neighborhoods)

print(f"CSV file saved to {csv_file_path}.")
