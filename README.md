# Socrata Open Data Datasets Inspector
Inspect all datasets in MD DoIT Open Data Portal on Socrata, inventory null data, and output statistics.

Use the Data Freshness Report dataset to identify all datasets to be inspected by this process.
For each dataset, request records from Socrata, inventory nulls in records, and repeat until all records have been
 processed.
Identify datasets without nulls, with nulls, and problem datasets that couldn't be processed.
Upsert output statistics to Socrata dataset providing an overview at the dataset level, a Socrata dataset
 providing information at the field level, a csv file capturing all problematic datasets, and a csv file
 reporting on the performance of the script. Optionally, also write dataset level and field level statistics to csv files.
Author: CJuice
Date: 20180601