# Socrata Open Data Datasets Inspector
Inspect all datasets in MD DoIT Open Data Portal on Socrata, inventory null data, and output statistics in .csv files.

Use the Data Freshness Report dataset to identify all datasets to be inspected by this process.
For each dataset, request records from Socrata, inventory nulls in records, and repeat until all records have been
 processed.
Identify datasets without nulls, with nulls, and problem datasets that couldn't be processed.
Output a csv file providing an overview of the entire process findings, a csv file providing information on each
 dataset with nulls including insight by column, a csv file capturing all problematic datasets, and a csv file
 reporting on the performance of the script.
Author: CJuice
Date: 20180501