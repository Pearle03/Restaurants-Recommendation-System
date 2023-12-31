import pandas as pd

# Load Business Data and Tips Data from CSV files (replace 'business.csv' and 'tips.csv' with your actual file paths)
business_data = pd.read_csv('yelp_academic_dataset_business.csv')
tips_data = pd.read_csv('yelp_academic_dataset_tip.csv')

# Merge 'city,' 'address,' and 'state' columns into 'full_address'
business_data['full_address'] = business_data['address'] + ', ' + business_data['city'] + ', ' + business_data['state']

# Remove unwanted quotes and commas
business_data['full_address'] = business_data['full_address'].str.replace('"', '')

# Define the columns to select from Business Data
business_columns = ['business_id', 'full_address', 'stars', 'categories', 'name', 'latitude', 'longitude']

# Filter Business Data
filtered_business_data = business_data[business_columns]

# Define the columns to select from Tips Data
tips_columns = ['business_id', 'text']

# Filter Tips Data
filtered_tips_data = tips_data[tips_columns]

# Merge (join) the filtered Business Data and Tips Data on 'Business_id'
merged_data = pd.merge(filtered_business_data, filtered_tips_data, on='business_id', how='inner')

# Add the "text" column to the business_columns list
business_columns.append('text')

# Reorder the columns in the merged data
merged_data = merged_data[business_columns]

# Write the merged data to a CSV file (replace 'output.csv' with your desired file path)
merged_data.to_csv('VegasRestaurantData.csv', index=False)

print("Data has been filtered, joined, and written to 'VegasRestaurantData.csv'")
