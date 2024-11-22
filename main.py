# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 20:05:50 2024

@author: PRO
"""

from pymongo import MongoClient
import pandas as pd
import re

# Connection string
connection_string = 'mongodb://localhost:27017/'

# checking connection's status
try:
    client = MongoClient(connection_string)
    client.admin.command('ping')  # Test the connection
    print("Connected to MongoDB!")
except Exception as e:
    print("Connection failed:", e)
        
# Connect to MongoDB
client = MongoClient(connection_string)

# Access the database and collection
db = client['purchases_large']  
collection = db['purchases_dataset']  

# Fetch all documents
documents = collection.find()

# Convert the documents to a list
data = list(documents)

# Load into a DataFrame
data_1 = pd.DataFrame(data)

# Display the DataFrame
print(data_1.head())

df = data_1.copy()

# Checking Dataframe shape
print(df.shape)
# 31 columns & 346018 rows
# Dataframe info 
df.info()
print(df.columns)

# Preprocessing 
# Drop rows with missing values in essential columns
essential_columns = ['Creation Date', 'Purchase Order Number', 'Acquisition Type']
df.dropna(subset=essential_columns, inplace=True)

# Handle missing values in moderately important columns
# Fill missing Total Price, Quantity, and Unit Price with 0 (or mean if preferred)
df['Total Price'].fillna(0, inplace=True)
df['Quantity'].fillna(0, inplace=True)
df['Unit Price'].fillna(df['Unit Price'].mode()[0], inplace=True)  # Fill with mode

# Handle missing values in other columns
df['Item Name'].fillna(df['Item Name'].mode()[0], inplace=True)  # Fill with mode
df['Item Description'].fillna('', inplace=True)  # Fill with empty string
df['Supplier Code'].fillna(df['Supplier Code'].mode()[0], inplace=True)  # Fill with mode
df['Supplier Name'].fillna(df['Supplier Name'].mode()[0], inplace=True)  # Fill with mode
df['Classification Codes'].fillna('', inplace=True)  # Fill with empty string
df['Normalized UNSPSC'].fillna(0, inplace=True)

# Handle columns with excessive missing values
# Consider dropping columns with too many missing values
columns_to_drop = [
    'LPA Number', 'Requisition Number', 'Supplier Zip Code', 
    'Location', 'Purchase Date', 'Supplier Qualifications', 
    'Sub-Acquisition Type', 'Sub-Acquisition Method', 
    'Commodity Title', 'Class', 'Class Title', 
    'Family', 'Family Title', 'Segment', 'Segment Title'
]
df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

# Convert 'Creation Date' column to datetime format
df['Creation Date'] = pd.to_datetime(df['Creation Date'])

# Clean price columns (remove dollar signs and commas, then convert to float)
df['Total Price'] = df['Total Price'].replace({'\$': '', ',': ''}, regex=True).astype(float)
df['Unit Price'] = df['Unit Price'].replace({'\$': '', ',': ''}, regex=True).astype(float)

# Preprocess 'department name' column
def preprocess_department(department):
    # Remove any trailing 'Department of', convert to lowercase, and strip leading/trailing whitespace
    department = re.sub(r',? *Department of$', '', department)  # Remove "Department of" and any trailing commas/spaces
    department = department.strip().lower()  # Convert to lowercase and strip extra spaces
    return department

# Assuming there is a column 'department_name' in your data
if 'Department Name' in df.columns:
    df['Department Name'] = df['Department Name'].apply(preprocess_department)

# Step 6: Final inspection
print(df.info())  # Check the final structure of the DataFrame
print(df.head())  # Preview the first few rows of the cleaned DataFrame

# Drop the existing collection and insert the cleaned data into MongoDB
collection.drop()
collection.insert_many(df.to_dict("records"))
