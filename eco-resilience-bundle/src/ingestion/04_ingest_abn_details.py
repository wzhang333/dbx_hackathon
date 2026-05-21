# Databricks notebook source
import urllib.request as req
from urllib.error import HTTPError
import os

# 1. Configuration (Updated for Repeatable Jobs)
# This creates a text box at the top of your notebook. 
# The 'abn' name MUST match the 'Key' you typed in the Workflows Task Parameters.
dbutils.widgets.text("abn", "80619661988") 
abn = dbutils.widgets.get("abn") 

history = 'N'
guid = 'c7e37559-0b78-4a36-8fff-849268aa6344'
volume_path = "/Volumes/eco_resilience/bronze/raw_abn_data"

# 2. Construct URL (Verified working structure)
url = (f"https://abr.business.gov.au/abrxmlsearch/AbrXmlSearch.asmx/"
       f"SearchByABNv202001?searchString={abn}"
       f"&includeHistoricalDetails={history}"
       f"&authenticationGuid={guid}")

# 3. Execute Request with Diagnostic Logging
try:
    print(f"Initiating request to ABR for ABN: {abn}...")
    conn = req.urlopen(url)
    returnedXML = conn.read()
    print("Request successful.")
except HTTPError as e:
    print(f"HTTP Error {e.code}: {e.reason}")
    returnedXML = e.read()
except Exception as e:
    print(f"Connection failed: {str(e)}")
    raise

# 4. Save to Bronze Volume
file_name = f"abn_raw_{abn}.xml"
full_path = os.path.join(volume_path, file_name)

with open(full_path, 'wb') as f:
    f.write(returnedXML)

print(f"File successfully ingested to Bronze: {full_path}")