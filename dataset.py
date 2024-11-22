# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 18:49:09 2024

@author: PRO
"""
#creating dataset for fine tuning 

import pandas as pd

# Expanded dataset with queries and intents
data = [
    # Fiscal Year
    {"user_input": "What is the total spending for fiscal year 2023?", "intent": "fiscal_year_spending"},
    {"user_input": "How many orders were placed in fiscal year 2022?", "intent": "fiscal_year_orders"},
    {"user_input": "Show me the most expensive item purchased in fiscal year 2021.", "intent": "fiscal_year_expensive_item"},
    {"user_input": "Which department spent the most in fiscal year 2020?", "intent": "fiscal_year_top_department"},
    
    # Purchase Order Number
    {"user_input": "Show me the details of purchase order PO12345.", "intent": "purchase_order_details"},
    {"user_input": "What is the total value of purchase order PO67890?", "intent": "purchase_order_value"},
    {"user_input": "Which supplier fulfilled purchase order PO98765?", "intent": "purchase_order_supplier"},
    {"user_input": "What items were ordered in PO54321?", "intent": "purchase_order_items"},
    
    # Acquisition Type
    {"user_input": "What is the total spending by acquisition type 'Contract'?", "intent": "acquisition_type_spending"},
    {"user_input": "How many orders were made under 'Open Market' acquisition type?", "intent": "acquisition_type_orders"},
    {"user_input": "Which department uses 'CalCard' the most?", "intent": "acquisition_type_department_usage"},
    {"user_input": "List the top suppliers for the 'Direct Purchase' acquisition type.", "intent": "acquisition_type_top_suppliers"},
    
    # Acquisition Method
    {"user_input": "Show me the total spending by acquisition method 'Bidding'.", "intent": "acquisition_method_spending"},
    {"user_input": "Which acquisition method has the highest average unit price?", "intent": "acquisition_method_avg_price"},
    {"user_input": "What is the most frequent acquisition method for IT items?", "intent": "acquisition_method_frequency"},
    {"user_input": "List the acquisition methods used by Department of Education.", "intent": "acquisition_method_department"},
    
    # Department Name
    {"user_input": "Which department made the most purchases in 2023?", "intent": "department_top_purchases"},
    {"user_input": "What is the spending breakdown for the Health Department?", "intent": "department_spending_breakdown"},
    {"user_input": "How many items were ordered by the Transportation Department?", "intent": "department_item_count"},
    {"user_input": "List all suppliers for the Education Department.", "intent": "department_suppliers"},
    
    # Supplier Code & Supplier Name
    {"user_input": "What is the total spending with supplier S1234?", "intent": "supplier_spending"},
    {"user_input": "Show me the items purchased from Supplier X.", "intent": "supplier_items"},
    {"user_input": "Which supplier has the highest total orders?", "intent": "supplier_top_orders"},
    {"user_input": "List all orders made with Supplier Y.", "intent": "supplier_orders"},
    
    # CalCard
    {"user_input": "What is the total spending using CalCard?", "intent": "calcard_total_spending"},
    {"user_input": "Which departments use CalCard the most?", "intent": "calcard_top_departments"},
    {"user_input": "Show me all orders made using CalCard.", "intent": "calcard_orders"},
    {"user_input": "Which items are frequently purchased using CalCard?", "intent": "calcard_frequent_items"},
    
    # Item Name & Item Description
    {"user_input": "What is the most frequently ordered item?", "intent": "frequent_items"},
    {"user_input": "Show me the details for item 'Laptop'.", "intent": "item_details"},
    {"user_input": "Which items have the highest unit price?", "intent": "expensive_items"},
    {"user_input": "List items purchased in bulk quantities.", "intent": "bulk_items"},
    
    # Quantity
    {"user_input": "What is the total quantity of items ordered?", "intent": "total_quantity"},
    {"user_input": "Show me all items ordered in quantities greater than 100.", "intent": "large_quantity_orders"},
    {"user_input": "Which department orders the largest quantity of items?", "intent": "quantity_top_department"},
    {"user_input": "What is the average quantity per order?", "intent": "avg_quantity_per_order"},
    
    # Unit Price
    {"user_input": "What is the average unit price for IT items?", "intent": "avg_unit_price_by_category"},
    {"user_input": "Which items have a unit price above $1,000?", "intent": "high_unit_price_items"},
    {"user_input": "Show me the unit price for item 'Desktop'.", "intent": "unit_price_item"},
    {"user_input": "What is the cheapest item purchased?", "intent": "cheapest_item"},
    
    # Total Price
    {"user_input": "Which order has the highest total price?", "intent": "highest_total_price_order"},
    {"user_input": "What is the total price of all orders in Q2 2023?", "intent": "total_price_by_quarter"},
    {"user_input": "Show me the total price of all IT-related items.", "intent": "total_price_by_category"},
    {"user_input": "Which supplier has the highest total revenue?", "intent": "supplier_top_revenue"},
    
    # Classification Codes
    {"user_input": "What is the spending breakdown by classification code?", "intent": "classification_spending_breakdown"},
    {"user_input": "List all items under classification code 12345.", "intent": "classification_items"},
    {"user_input": "Which classification code has the highest total spending?", "intent": "top_classification_code"},
    {"user_input": "Show me the most frequently purchased items in classification code 67890.", "intent": "classification_frequent_items"},
    {"user_input": "What is the highest spending quarter?", "intent": "show_highest_spending_quarter"},
    {"user_input": "Show me the quarter with the most spending.", "intent": "show_highest_spending_quarter"},
    {"user_input": "Which quarter had the highest expenditure?", "intent": "show_highest_spending_quarter"},
    {"user_input": "How many orders were made last month?", "intent": "total_orders"},
    {"user_input": "Tell me the total number of orders.", "intent": "total_orders"},
    {"user_input": "List the most frequently ordered items.", "intent": "frequent_items"},
    {"user_input": "What is the spending by each department?", "intent": "department_spending"},
    {"user_input": "Show spending details per department.", "intent": "department_spending"},
    {"user_input": "Tell me the spending by acquisition type.", "intent": "acquisition_spending"},
    {"user_input": "What’s the total quantity of items purchased?", "intent": "total_quantity"},
    {"user_input": "What orders were placed with Supplier X?", "intent": "supplier_orders"},
]

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
csv_path = "procurement_intents_expanded.csv"
df.to_csv(csv_path, index=False)
csv_path

