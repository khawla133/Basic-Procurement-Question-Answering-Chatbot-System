# MongoDB Connection
from datetime import datetime, timedelta
from pymongo import MongoClient
import re
from dateutil.parser import parse
from dateparser import parse
import logging
def connect_to_mongodb(connection_string, db_name, collection_name):
    try:
        client = MongoClient(connection_string)
        client.admin.command('ping')
        print("Connected to MongoDB!")
        db = client[db_name]
        collection = db[collection_name]
        return collection
    except Exception as e:
        print("Connection failed:", e)
        raise
def get_highest_spending_quarter(collection):
    """
    Identify the fiscal quarter with the highest spending.
    
    Returns:
    - dict: Information about the highest spending quarter with total expenditure.
    """
    pipeline = [
        {
            "$addFields": {
                "quarter": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lte": [{"$month": "$Creation Date"}, 3]}, "then": "Q1"},
                            {"case": {"$lte": [{"$month": "$Creation Date"}, 6]}, "then": "Q2"},
                            {"case": {"$lte": [{"$month": "$Creation Date"}, 9]}, "then": "Q3"},
                        ],
                        "default": "Q4"
                    }
                }
            }
        },
        {"$group": {"_id": "$quarter", "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"total_spending": -1}},
        {"$limit": 1}
    ]
    result = list(collection.aggregate(pipeline))
    return result[0] if result else {}

# Reusable function to execute pipelines
def execute_pipeline(collection, pipeline):
    try:
        return list(collection.aggregate(pipeline))
    except Exception as e:
        print("Pipeline execution failed:", e)
        return []

# Query Functions
def get_total_orders(collection, start_date, end_date):
    """
    Get the total number of orders placed between the specified date range.

    Args:
    - collection: MongoDB collection object.
    - start_date (str): Start date in 'YYYY-MM-DD' format.
    - end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
    - int: Total number of orders within the date range.
    """
    try:
        # Convert date strings to datetime objects
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # MongoDB aggregation pipeline
        pipeline = [
            {"$match": {"Creation Date": {"$gte": start_date, "$lte": end_date}}},  # Filter by date range
            {"$count": "total_orders"}  # Count the documents
        ]

        # Execute pipeline
        result = list(collection.aggregate(pipeline))
        if result:
            return result[0]["total_orders"]  # Return the count
        else:
            return 0  # No orders found in the range
    except Exception as e:
        raise ValueError(f"Error fetching total orders: {e}")


def get_frequent_line_items(collection, top_n=5):
    """
    Fetch the most frequent line items in the database.

    Args:
    - collection: MongoDB collection object.
    - top_n (int): Number of top items to retrieve.

    Returns:
    - List[Dict]: List of line items with their frequency.
    """
    pipeline = [
        {"$group": {"_id": "$Item Name", "frequency": {"$sum": 1}}},
        {"$sort": {"frequency": -1}},
        {"$limit": top_n}
    ]
    result = execute_pipeline(collection, pipeline)
    return [
        {"Item Name": item["_id"], "Frequency": item["frequency"]}
        for item in result
    ]


def get_total_quantity(collection):
    """
    Calculate the total quantity of all items in the database.

    Args:
    - collection: MongoDB collection object.

    Returns:
    - Dict: A dictionary containing the total quantity or an error message.
    """
    try:
        # Aggregate the total quantity from the database
        pipeline = [
            {"$group": {"_id": None, "total_quantity": {"$sum": "$Quantity"}}}
        ]
        result = execute_pipeline(collection, pipeline)

        # Ensure result is valid
        total_quantity = result[0]["total_quantity"] if result and "total_quantity" in result[0] else 0

        # Return the numeric result as part of a dictionary
        return {"success": True, "total_quantity": int(total_quantity)}
    except Exception as e:
        # Return an error dictionary on failure
        return {"success": False, "message": f"An error occurred: {str(e)}"}




def get_orders_by_supplier(collection, supplier_name):
    """
    Fetch all orders for a given supplier in a readable format.

    Args:
    - collection: MongoDB collection object.
    - supplier_name (str): The name of the supplier.

    Returns:
    - List[Dict]: List of orders for the supplier.
    """
    if not supplier_name:
        return [{"Message": "No supplier name provided. Please specify a supplier."}]

    supplier_name = supplier_name.strip()

    try:
        orders = list(collection.find(
            {"Supplier Name": {"$regex": f"^{supplier_name}$", "$options": "i"}},
            {"_id": 0, "Purchase Order Number": 1, "Total Price": 1, "Creation Date": 1}
        ))

        if orders:
            formatted_orders = []
            for order in orders:
                # Handle cases where Total Price is a string or a number
                total_price = order.get("Total Price", 0)
                if isinstance(total_price, (int, float)):
                    formatted_total_price = f"${total_price:,.2f}"
                else:
                    formatted_total_price = total_price  # Assume it's already formatted

                # Handle Creation Date formatting
                creation_date = order.get("Creation Date")
                if isinstance(creation_date, datetime):
                    formatted_creation_date = creation_date.strftime("%Y-%m-%d")
                else:
                    formatted_creation_date = "N/A"

                formatted_orders.append({
                    "Purchase Order Number": order.get("Purchase Order Number", "N/A"),
                    "Total Price": formatted_total_price,
                    "Creation Date": formatted_creation_date,
                })

            return formatted_orders
        else:
            return [{"Message": f"No orders found for supplier: {supplier_name}."}]
    except Exception as e:
        return [{"Message": f"Error fetching orders for supplier {supplier_name}: {str(e)}"}]


    
    
# Acquisition Methods
def get_acquisition_method_avg_price(collection):
    pipeline = [
        {"$group": {"_id": "$Acquisition Method", "avg_price": {"$avg": "$Unit Price"}}},
        {"$sort": {"avg_price": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Acquisition Method": result["_id"], "Average Price": round(result["avg_price"], 2)}
        for result in results
    ]

def get_acquisition_method_department(collection):
    pipeline = [
        {"$group": {"_id": {"method": "$Acquisition Method", "department": "$Department Name"}, 
                    "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"_id.method": 1, "_id.department": 1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Acquisition Method": result["_id"]["method"], "Department": result["_id"]["department"], 
         "Total Spending": round(result["total_spending"], 2)}
        for result in results
    ]

def get_acquisition_method_frequency(collection):
    pipeline = [
        {"$group": {"_id": "$Acquisition Method", "frequency": {"$sum": 1}}},
        {"$sort": {"frequency": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Acquisition Method": result["_id"], "Frequency": result["frequency"]}
        for result in results
    ]

def get_acquisition_method_spending(collection):
    pipeline = [
        {"$group": {"_id": "$Acquisition Method", "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"total_spending": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Acquisition Method": result["_id"], "Total Spending": round(result["total_spending"], 2)}
        for result in results
    ]

# Acquisition Types
def get_acquisition_spending(collection):
    pipeline = [
        {"$group": {"_id": "$Acquisition Type", "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"total_spending": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Acquisition Type": result["_id"], "Total Spending": round(result["total_spending"], 2)}
        for result in results
    ]

def get_acquisition_type_department_usage(collection):
    pipeline = [
        {"$group": {"_id": {"type": "$Acquisition Type", "department": "$Department Name"}, 
                    "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"_id.type": 1, "_id.department": 1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Acquisition Type": result["_id"]["type"], "Department": result["_id"]["department"], 
         "Total Spending": round(result["total_spending"], 2)}
        for result in results
    ]

def get_acquisition_type_orders(collection):
    pipeline = [
        {"$group": {"_id": "$Acquisition Type", "total_orders": {"$sum": 1}}},
        {"$sort": {"total_orders": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Acquisition Type": result["_id"], "Total Orders": result["total_orders"]}
        for result in results
    ]

def get_acquisition_type_top_suppliers(collection):
    pipeline = [
        {"$group": {"_id": {"type": "$Acquisition Type", "supplier": "$Supplier Name"}, 
                    "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"total_spending": -1}},
        {"$limit": 10}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Acquisition Type": result["_id"]["type"], "Supplier": result["_id"]["supplier"], 
         "Total Spending": round(result["total_spending"], 2)}
        for result in results
    ]

# Quantity and Unit Price
def get_avg_quantity_per_order(collection):
    pipeline = [
        {"$group": {"_id": None, "avg_quantity": {"$avg": "$Quantity"}}}
    ]
    result = execute_pipeline(collection, pipeline)
    return {"Average Quantity Per Order": round(result[0]["avg_quantity"], 2)} if result else {}

def get_avg_unit_price_by_category(collection):
    pipeline = [
        {"$group": {"_id": "$Classification Codes", "avg_unit_price": {"$avg": "$Unit Price"}}},
        {"$sort": {"avg_unit_price": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Classification Code": result["_id"], "Average Unit Price": round(result["avg_unit_price"], 2)}
        for result in results
    ]

def get_bulk_items(collection):
    pipeline = [
        {"$match": {"Quantity": {"$gte": 100}}},  # Define bulk as >= 100 items
        {"$sort": {"Quantity": -1}},
        {"$limit": 10}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Item Name": result["Item Name"], "Quantity": result["Quantity"]}
        for result in results
    ]

def get_high_unit_price_items(collection, threshold=1000, top_n=10):
    """
    Fetch items with unit prices greater than the specified threshold.

    Args:
    - collection: MongoDB collection object.
    - threshold (float): The minimum unit price to filter by.
    - top_n (int): Number of top items to retrieve.

    Returns:
    - List[Dict]: List of items with their unit prices, sorted in descending order.
    """
    pipeline = [
        {"$match": {"Unit Price": {"$gt": threshold}}},  # Filter items with unit prices > threshold
        {"$sort": {"Unit Price": -1}},                  # Sort by unit price in descending order
        {"$limit": top_n}                               # Limit to the top N results
    ]
    
    results = execute_pipeline(collection, pipeline)
    
    # Format results for readability
    return [
        {
            "Item Name": result.get("Item Name", "Unknown Item"),
            "Unit Price": f"${result['Unit Price']:,.2f}",
            "Purchase Order Number": result.get("Purchase Order Number", "Unknown Order")
        }
        for result in results
    ]


# CalCard
def get_calcard_frequent_items(collection):
    pipeline = [
        {"$group": {"_id": "$Item Name", "frequency": {"$sum": 1}}},
        {"$sort": {"frequency": -1}},
        {"$limit": 10}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Item Name": result["_id"], "Frequency": result["frequency"]}
        for result in results
    ]

def get_calcard_orders(collection):
    pipeline = [
        {"$group": {"_id": "$CalCard", "total_orders": {"$sum": 1}}},
        {"$sort": {"total_orders": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"CalCard": result["_id"], "Total Orders": result["total_orders"]}
        for result in results
    ]

def get_calcard_top_departments(collection):
    pipeline = [
        {"$group": {"_id": {"CalCard": "$CalCard", "department": "$Department Name"}, 
                    "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"total_spending": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"CalCard": result["_id"]["CalCard"], "Department": result["_id"]["department"], 
         "Total Spending": round(result["total_spending"], 2)}
        for result in results
    ]

def get_calcard_total_spending(collection):
    pipeline = [
        {"$group": {"_id": "$CalCard", "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"total_spending": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"CalCard": result["_id"], "Total Spending": round(result["total_spending"], 2)}
        for result in results
    ]


# Miscellaneous
def get_cheapest_item(collection):
    """
    Find the cheapest item in the database.

    Args:
    - collection: MongoDB collection object.

    Returns:
    - dict: Readable details of the cheapest item.
    """
    # Query to find the item with the lowest unit price
    cheapest_item = collection.find_one(
        sort=[("Unit Price", 1)]  # Sort by Unit Price in ascending order
    )

    if cheapest_item:
        # Return only the most relevant details in a readable format
        return {
            "Item Name": cheapest_item.get("Item Name", "N/A"),
            "Unit Price": cheapest_item.get("Unit Price", "N/A"),
            "Department Name": cheapest_item.get("Department Name", "N/A"),
            "Supplier Name": cheapest_item.get("Supplier Name", "N/A"),
            "Purchase Order Number": cheapest_item.get("Purchase Order Number", "N/A"),
            "Description": cheapest_item.get("Item Description", "N/A"),
        }

    return None


def get_highest_total_price_order(collection):
    pipeline = [
        {"$group": {"_id": "$Purchase Order Number", "total_price": {"$sum": "$Total Price"}}},
        {"$sort": {"total_price": -1}},
        {"$limit": 1}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Purchase Order Number": result["_id"], "Total Price": round(result["total_price"], 2)}
        for result in results
    ]


def get_large_quantity_orders(collection):
    pipeline = [
        {"$match": {"Quantity": {"$gte": 50}}},  
        {"$sort": {"Quantity": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Item Name": result["Item Name"], "Quantity": result["Quantity"], 
         "Purchase Order Number": result["Purchase Order Number"]}
        for result in results
    ]


def get_total_price_by_category(collection):
    pipeline = [
        {"$group": {"_id": "$Classification Codes", "total_price": {"$sum": "$Total Price"}}},
        {"$sort": {"total_price": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Classification Code": result["_id"], "Total Price": round(result["total_price"], 2)}
        for result in results
    ]


def get_total_price_by_quarter(collection):
    pipeline = [
        {"$addFields": {"quarter": {"$ceil": {"$divide": [{"$month": "$Creation Date"}, 3]}}}},
        {"$group": {"_id": "$quarter", "total_price": {"$sum": "$Total Price"}}},
        {"$sort": {"_id": 1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Quarter": f"Q{result['_id']}", "Total Price": round(result["total_price"], 2)}
        for result in results
    ]


def get_classification_frequent_items(collection, top_n=10):
    pipeline = [
        {"$group": {"_id": "$Classification Codes", "frequency": {"$sum": 1}}},
        {"$sort": {"frequency": -1}},
        {"$limit": top_n}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Classification Code": result["_id"], "Frequency": result["frequency"]}
        for result in results
    ]


def get_classification_items(collection, classification_code=None):
    pipeline = []
    if classification_code:
        pipeline.append({"$match": {"Classification Codes": classification_code}})
    
    pipeline.extend([
        {"$group": {"_id": {"classification_code": "$Classification Codes", "item": "$Item Name"},
                    "total_quantity": {"$sum": "$Quantity"}}},
        {"$sort": {"total_quantity": -1}}
    ])
    
    results = execute_pipeline(collection, pipeline)
    return [
        {"Classification Code": result["_id"]["classification_code"], 
         "Item Name": result["_id"]["item"], 
         "Total Quantity": result["total_quantity"]}
        for result in results
    ]


def get_classification_spending_breakdown(collection):
    pipeline = [
        {"$group": {"_id": "$Classification Codes", "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"total_spending": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Classification Code": result["_id"], "Total Spending": round(result["total_spending"], 2)}
        for result in results
    ]


def get_top_classification_code(collection):
    """
    Fetch the classification code with the highest total spending from MongoDB.

    Args:
    - collection: MongoDB collection object.

    Returns:
    - Dict: The classification code and its total spending.
    """
    try:
        pipeline = [
            # Group by classification code and sum the total price
            {"$group": {"_id": "$Classification Codes", "total_spending": {"$sum": "$Total Price"}}},
            # Sort by total spending in descending order
            {"$sort": {"total_spending": -1}},
            # Limit the result to the top classification code
            {"$limit": 1}
        ]
        # Execute the pipeline
        result = list(collection.aggregate(pipeline))

        # If a result is found, return the classification code and total spending
        if result:
            return {
                "Classification Code": result[0]["_id"],
                "Total Spending": round(result[0]["total_spending"], 2)
            }
        else:
            return {"Message": "No data found for classification codes."}
    except Exception as e:
        return {"Error": f"An error occurred while fetching the top classification code: {str(e)}"}




# Define a helper function to format monetary values and large numbers
def format_currency(value):
    return f"${value:,.2f}"

def format_large_number(value):
    return f"{value:,}"

# 1. Department Functions
def get_department_item_count(collection):
    pipeline = [
        {"$group": {"_id": "$Department Name", "total_item_count": {"$sum": "$Quantity"}}},
        {"$sort": {"total_item_count": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Department Name": result["_id"], "Total Item Count": format_large_number(result["total_item_count"])}
        for result in results
    ]

def get_department_spending_breakdown(collection):
    pipeline = [
        {"$group": {"_id": "$Department Name", "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"total_spending": -1}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Department Name": result["_id"], "Total Spending": format_currency(result["total_spending"])}
        for result in results
    ]

def get_department_suppliers(collection, department_name):
    """
    Fetch suppliers for a specific department.
    """
    if not department_name:
        return [{"Message": "No department name provided. Please specify a department."}]
    
    pipeline = [
        {"$match": {"Department Name": {"$regex": f"^{department_name}$", "$options": "i"}}},
        {"$group": {"_id": "$Supplier Name"}},
        {"$project": {"Supplier Name": "$_id", "_id": 0}}
    ]
    
    try:
        results = execute_pipeline(collection, pipeline)
        return results if results else [{"Message": f"No suppliers found for department: {department_name}."}]
    except Exception as e:
        return [{"Message": f"Error fetching suppliers for department: {str(e)}"}]



def get_department_top_purchases(collection, query, top_n=10):
    department_name = extract_department_from_query(query)
    if not department_name:
        return [{"Message": "Department name not found in the query."}]

    pipeline = [
        {"$match": {"Department Name": department_name}},
        {"$group": {"_id": "$Item Name", "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"total_spending": -1}},
        {"$limit": top_n}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Item Name": result["_id"], "Total Spending": format_currency(result["total_spending"])}
        for result in results
    ]

def get_quantity_top_department(collection):
    """
    Fetch the department with the highest total quantity of items from MongoDB.

    Args:
    - collection: MongoDB collection object.

    Returns:
    - Dict: The department with the highest quantity and its total quantity.
    """
    try:
        pipeline = [
            # Group by department and sum the quantities
            {"$group": {"_id": "$Department Name", "total_quantity": {"$sum": "$Quantity"}}},
            # Sort by total quantity in descending order
            {"$sort": {"total_quantity": -1}},
            # Limit the result to the top department
            {"$limit": 1}
        ]
        # Execute the pipeline
        result = list(collection.aggregate(pipeline))

        # If a result is found, return the department name and total quantity
        if result:
            return {
                "Department Name": result[0]["_id"],
                "Total Quantity": result[0]["total_quantity"]
            }
        else:
            return {"Message": "No data found for department quantities."}
    except Exception as e:
        return {"Error": f"An error occurred while fetching the top department: {str(e)}"}


# 2. Fiscal Year Functions
def get_fiscal_year_spending(collection, query):
    fiscal_year = extract_fiscal_year_from_query(query)
    if not fiscal_year:
        return [{"Message": "Fiscal year not found in the query."}]

    pipeline = [
        {"$match": {"Fiscal Year": {"$regex": fiscal_year}}},
        {"$group": {"_id": None, "total_spending": {"$sum": "$Total Price"}}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [{"Fiscal Year": fiscal_year, "Total Spending": results[0]["total_spending"]}] if results else []


def get_fiscal_year_top_department(collection, query):
    fiscal_year = extract_fiscal_year_from_query(query)
    if not fiscal_year:
        return [{"Message": "Fiscal year not found in the query."}]

    pipeline = [
        {"$match": {"Fiscal Year": {"$regex": fiscal_year}}},
        {"$group": {"_id": "$Department Name", "total_spending": {"$sum": "$Total Price"}}},
        {"$sort": {"total_spending": -1}},
        {"$limit": 1}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Department Name": results[0]["_id"], "Total Spending": format_currency(results[0]["total_spending"])}
    ] if results else []

def get_fiscal_year_expensive_item(collection, query):
    """
    Get the most expensive item in a specific fiscal year based on the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the fiscal year.

    Returns:
    - List[Dict]: Details of the most expensive item in the fiscal year, or a message if no data is found.
    """
    # Extract fiscal year from the query
    fiscal_year = extract_fiscal_year_from_query(query)

    if not fiscal_year:
        return [{"Message": "Fiscal year not found in the query. Could you please specify the fiscal year?"}]

    # Aggregation pipeline to find the most expensive item for the fiscal year
    pipeline = [
        {"$match": {"Fiscal Year": fiscal_year}},  # Match the exact fiscal year
        {"$sort": {"Unit Price": -1}},  # Sort by unit price in descending order
        {"$limit": 1}  # Get the most expensive item
    ]

    result = execute_pipeline(collection, pipeline)

    if result:
        return [
            {
                "Item Name": result[0].get("Item Name", "N/A"),
                "Unit Price": f"${result[0].get('Unit Price', 0):,.2f}",
                "Purchase Order Number": result[0].get("Purchase Order Number", "N/A"),
                "Department Name": result[0].get("Department Name", "N/A")
            }
        ]
    else:
        return [{"Message": f"No data found for fiscal year: {fiscal_year}."}]

def get_fiscal_year_orders(collection, query):
    """
    Get the total number of orders in a specific fiscal year based on the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the fiscal year.

    Returns:
    - List[Dict]: A list containing the fiscal year and total number of orders, or a message if not found.
    """
    # Extract fiscal year from the user's query
    fiscal_year = extract_fiscal_year_from_query(query)
    
    if not fiscal_year:
        return [{"Message": "Fiscal year not found in the query. Could you please clarify?"}]
    
    # Define the aggregation pipeline to count orders in the specified fiscal year
    pipeline = [
        {"$match": {"Fiscal Year": fiscal_year}},  # Match the specified fiscal year
        {"$group": {"_id": None, "total_orders": {"$sum": 1}}}  # Count the total orders
    ]
    
    # Execute the aggregation pipeline
    result = execute_pipeline(collection, pipeline)
    
    # Prepare the result in a readable format
    if result:
        total_orders = result[0]["total_orders"]
        return [{"Message": f"The total number of orders placed in fiscal year {fiscal_year} is {total_orders}."}]
    else:
        return [{"Message": f"No orders found for the fiscal year {fiscal_year}."}]


# 3. Supplier Functions
def get_supplier_spending(collection, query):
    supplier_name = extract_supplier_name_from_query(collection, query)
    if not supplier_name:
        return [{"Message": "Supplier name not found in the query."}]

    pipeline = [
        {"$match": {"Supplier Name": supplier_name}},
        {"$group": {"_id": None, "total_spending": {"$sum": "$Total Price"}}}
    ]
    results = execute_pipeline(collection, pipeline)
    return [{"Supplier Name": supplier_name, "Total Spending": format_currency(results[0]["total_spending"])}] if results else []

def get_supplier_top_orders(collection, query, top_n=10):
    supplier_name = extract_supplier_name_from_query(collection, query)
    if not supplier_name:
        return [{"Message": "Supplier name not found in the query."}]

    pipeline = [
        {"$match": {"Supplier Name": supplier_name}},
        {"$group": {"_id": "$Purchase Order Number", "total_order_value": {"$sum": "$Total Price"}}},
        {"$sort": {"total_order_value": -1}},
        {"$limit": top_n}
    ]
    results = execute_pipeline(collection, pipeline)
    return [
        {"Purchase Order Number": result["_id"], "Order Value": format_currency(result["total_order_value"])}
        for result in results
    ]


def get_top_suppliers(collection,limit=3):
    result = collection.suppliers.aggregate([
        {"$group": {"_id": "$supplier_name", "total_spending": {"$sum": "$spending"}}},
        {"$sort": {"total_spending": -1}},
        {"$limit": limit}
    ])
    suppliers = list(result)
    if suppliers:
        response = "\n".join([f"{idx + 1}. {supplier['_id']}: ${supplier['total_spending']:,.2f}" for idx, supplier in enumerate(suppliers)])
        return f"The top suppliers by spending are:\n{response}"
    return "No supplier data found."


def get_supplier_order_count(collection, query):
    """
    Fetch the order count for a specific supplier based on the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the supplier name.

    Returns:
    - str: Response containing the total order count for the supplier.
    """
    # Extract supplier name from the query
    supplier_name = extract_supplier_name_from_query(query, collection)

    if supplier_name:
        # Count the number of orders for the extracted supplier name
        order_count = collection.orders.count_documents({"Supplier Name": supplier_name})

        if order_count:
            return f"A total of {order_count} orders were placed with {supplier_name}."
        else:
            return f"No orders found for supplier: {supplier_name}."
    else:
        return "I couldn't find a valid supplier name in your query. Could you please clarify?"

def get_supplier_top_revenue(collection, top_n=10):
    """
    Fetch suppliers with the highest total revenue.

    Args:
    - collection: MongoDB collection object.
    - top_n (int): Number of top suppliers to retrieve.

    Returns:
    - List[Dict]: List of suppliers with their total revenue.
    """
    pipeline = [
        {"$group": {"_id": "$Supplier Name", "total_revenue": {"$sum": "$Total Price"}}},
        {"$sort": {"total_revenue": -1}},
        {"$limit": top_n}
    ]
    result = execute_pipeline(collection, pipeline)
    return [{"supplier_name": item["_id"], "total_revenue": item["total_revenue"]} for item in result]

def get_supplier_items(collection, query):
    """
    Fetch items provided by a specific supplier based on the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the supplier name.

    Returns:
    - str: Response containing the list of items provided by the supplier.
    """
    # Extract supplier name from the query
    supplier_name = extract_supplier_name_from_query(collection, query)

    if supplier_name:
        # Perform the aggregation query to get items provided by the supplier
        pipeline = [
            {"$match": {"Supplier Name": supplier_name}},
            {"$group": {"_id": "$Item Name", "total_quantity": {"$sum": "$Quantity"}}},
            {"$sort": {"total_quantity": -1}}
        ]
        result = execute_pipeline(collection, pipeline)

        if result:
            # Prepare the response with the supplier's items
            items = "\n".join([f"{item['item_name']} (Quantity: {item['total_quantity']})" for item in result])
            return f"Items provided by supplier {supplier_name}:\n{items}"
        else:
            return f"No items found for supplier: {supplier_name}"
    else:
        return "I couldn't find a valid supplier name in your query. Could you please clarify?"


# 4. Item and Order Functions



def get_spending_by_acquisition_type(collection, query):

    """
    Fetch the total spending for a specific acquisition type based on the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the acquisition type.

    Returns:
    - List[Dict]: Readable results containing acquisition type and total spending.
    """
    # Extract acquisition type from the query
    acquisition_type = extract_acquisition_type_from_query(collection, query)

    if not acquisition_type:
        return [{"Message": "Acquisition type not found in the query. Could you please clarify?"}]

    # Perform the aggregation query if the acquisition type is found
    pipeline = [
        {"$match": {"Acquisition Type": acquisition_type}},
        {"$group": {"_id": "$Acquisition Type", "total_spending": {"$sum": "$Total Price"}}}
    ]
    results = execute_pipeline(collection, pipeline)

    if results:
        return [
            {
                "Acquisition Type": result["_id"],
                "Total Spending": f"${result['total_spending']:,.2f}"
            }
            for result in results
        ]
    else:
        return [{"Message": f"No spending data found for acquisition type: {acquisition_type}."}]
def get_item_details(collection, query):

    """
    Fetch details of an item based on the extracted item name from the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the item name.

    Returns:
    - List[Dict]: List of item details.
    """
    # Extract item name from the query
    item_name = extract_item_name_from_query(query)

    if item_name:
        pipeline = [
            {"$match": {"Item Name": item_name}}
        ]
        result = execute_pipeline(collection, pipeline)
        return result
    else:
        return "No item name found in the query. Please clarify."


def get_purchase_order_details(collection, query):
    """
    Fetch details of a purchase order based on the extracted purchase order number.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the purchase order number.

    Returns:
    - List[Dict]: List of purchase order details.
    """
    # Extract purchase order number from the query
    purchase_order_number = extract_purchase_order_number_from_query(query)

    if purchase_order_number:
        pipeline = [
            {"$match": {"Purchase Order Number": purchase_order_number}}
        ]
        result = execute_pipeline(collection, pipeline)
        return result
    else:
        return "No purchase order number found in the query. Please clarify."
    
    
def get_purchase_order_supplier(collection, query):
    """
    Fetch the supplier for a given purchase order number from the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the purchase order number.

    Returns:
    - List[Dict]: List of suppliers for the purchase order.
    """
    # Extract purchase order number from the query
    purchase_order_number = extract_purchase_order_number_from_query(query)

    if purchase_order_number:
        pipeline = [
            {"$match": {"Purchase Order Number": purchase_order_number}},
            {"$group": {"_id": "$Supplier Name"}}
        ]
        result = execute_pipeline(collection, pipeline)
        return [{"supplier_name": item["_id"]} for item in result]
    else:
        return "No purchase order number found in the query. Please clarify."

def get_purchase_order_value(collection, query):
    """
    Fetch the total value for a given purchase order number from the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the purchase order number.

    Returns:
    - float: Total value for the purchase order.
    """
    # Extract purchase order number from the query
    purchase_order_number = extract_purchase_order_number_from_query(query)

    if purchase_order_number:
        pipeline = [
            {"$match": {"Purchase Order Number": purchase_order_number}},
            {"$group": {"_id": None, "total_value": {"$sum": "$Total Price"}}}
        ]
        result = execute_pipeline(collection, pipeline)
        return result[0]["total_value"] if result else 0
    else:
        return "No purchase order number found in the query. Please clarify."

def get_purchase_order_items(collection, query):
    """
    Fetch items in a specific purchase order based on the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the purchase order number.

    Returns:
    - str: Response containing the list of items for the purchase order.
    """
    # Extract purchase order number from the query
    purchase_order_number = extract_purchase_order_number_from_query(query)

    if purchase_order_number:
        # Perform the aggregation query to get items in the purchase order
        pipeline = [
            {"$match": {"Purchase Order Number": purchase_order_number}},
            {"$group": {"_id": "$Item Name", "total_quantity": {"$sum": "$Quantity"}}}
        ]
        result = execute_pipeline(collection, pipeline)

        if result:
            # Prepare the response with the items in the purchase order
            items = "\n".join([f"{item['item_name']} (Quantity: {item['total_quantity']})" for item in result])
            return f"Items in purchase order {purchase_order_number}:\n{items}"
        else:
            return f"No items found for purchase order number: {purchase_order_number}"
    else:
        return "I couldn't find a valid purchase order number in your query. Could you please clarify?"

def get_supplier_items(collection, query):
    """
    Fetch items provided by a specific supplier based on the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the supplier name.

    Returns:
    - str: Response containing the list of items provided by the supplier.
    """
    # Extract supplier name from the query
    supplier_name = extract_supplier_name_from_query(collection, query)

    if supplier_name:
        # Perform the aggregation query to get items provided by the supplier
        pipeline = [
            {"$match": {"Supplier Name": supplier_name}},
            {"$group": {"_id": "$Item Name", "total_quantity": {"$sum": "$Quantity"}}},
            {"$sort": {"total_quantity": -1}}
        ]
        result = execute_pipeline(collection, pipeline)

        if result:
            # Prepare the response with the supplier's items
            items = "\n".join([f"{item['item_name']} (Quantity: {item['total_quantity']})" for item in result])
            return f"Items provided by supplier {supplier_name}:\n{items}"
        else:
            return f"No items found for supplier: {supplier_name}"
    else:
        return "I couldn't find a valid supplier name in your query. Could you please clarify?"


def get_unit_price_item(collection, query):
    """
    Fetch the unit price for a specific item based on the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the item name.

    Returns:
    - Dict: A readable result containing item details or an error message.
    """
    try:
        # Extract item name from the query
        item_name = extract_item_name_from_query(query)

        if not item_name:
            return {"Message": "Item name not found in the query. Could you please clarify?"}

        # Query the database for the specific item
        pipeline = [
            {"$match": {"Item Name": {"$regex": f"^{item_name}$", "$options": "i"}}},  # Case-insensitive exact match
            {"$project": {
                "Item Name": 1,
                "Unit Price": 1,
                "Department Name": 1,
                "Supplier Name": 1,
                "Purchase Order Number": 1,
                "_id": 0
            }}
        ]
        result = list(collection.aggregate(pipeline))

        if result:
            # Format the response to be more readable
            return [
                {
                    "Item Name": item.get("Item Name", "N/A"),
                    "Unit Price": f"${item.get('Unit Price', 0):,.2f}",
                    "Department Name": item.get("Department Name", "N/A"),
                    "Supplier Name": item.get("Supplier Name", "N/A"),
                    "Purchase Order Number": item.get("Purchase Order Number", "N/A")
                }
                for item in result
            ]
        else:
            return {"Message": f"No data found for the item: {item_name}"}
    except Exception as e:
        return {"Error": f"An error occurred while fetching the item details: {str(e)}"}



def get_department_spending_by_name(collection, query):
    """
    Fetch the total spending for a specific department based on the user's query.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the department name.

    Returns:
    - Dict: Readable result containing the department name and total spending, or an error message.
    """
    try:
        # Extract the department name from the query
        department_name = query

        if not department_name:
            return {"Message": "Department name not found in the query. Could you please clarify?"}

        # Query the database for the total spending of the specified department
        pipeline = [
            {"$match": {"Department Name": {"$regex": f"^{department_name}$", "$options": "i"}}},  # Case-insensitive exact match
            {"$group": {"_id": "$Department Name", "Total Spending": {"$sum": "$Total Price"}}}
        ]
        result = list(collection.aggregate(pipeline))

        if result:
            # Format the result into a readable form
            return {
                "Department Name": result[0]["_id"],
                "Total Spending": f"${result[0]['Total Spending']:,.2f}"
            }
        else:
            return {"Message": f"No spending data found for department: {department_name}"}
    except Exception as e:
        return {"Error": f"An error occurred while fetching the department spending: {str(e)}"}

def get_highest_spending_department(collection):
    """
    Fetch the department with the highest total spending.

    Args:
    - collection: MongoDB collection object.

    Returns:
    - Dict: Readable result containing the department name and total spending, or an error message.
    """
    try:
        # MongoDB aggregation pipeline to find the department with the highest spending
        pipeline = [
            {"$group": {"_id": "$Department Name", "Total Spending": {"$sum": "$Total Price"}}},
            {"$sort": {"Total Spending": -1}},  # Sort by total spending in descending order
            {"$limit": 1}  # Limit to the top department
        ]
        result = list(collection.aggregate(pipeline))

        if result:
            # Format the result for readability
            return {
                "Department Name": result[0]["_id"],
                "Total Spending": f"${result[0]['Total Spending']:,.2f}"
            }
        else:
            return {"Message": "No spending data found for any department."}
    except Exception as e:
        return {"Error": f"An error occurred while fetching the highest spending department: {str(e)}"}





# 6. User-Friendly Features
def handle_greeting(collection=None):
    return "Hi! How can I assist you today?"

def handle_unknown_query():
    return "I'm sorry, I couldn't understand your query. Could you please rephrase?"


# Functuins to extract specific information from user's query

def extract_dates_from_query(query):
    """
    Extract possible dates or date ranges from the user's query.
    Supports ISO format, natural language ranges, and various patterns.
    """
    query = query.lower()
    print("Query:", query)  # Debugging query input

    dates = []

    # Handle natural language date ranges (e.g., "last year", "this year")
    now = datetime.now()
    if "last year" in query:
        start_date = datetime(now.year - 1, 1, 1).strftime('%Y-%m-%d')
        end_date = datetime(now.year - 1, 12, 31).strftime('%Y-%m-%d')
        print("Matched 'last year':", [start_date, end_date])  # Debugging output
        return [start_date, end_date]

    if "this year" in query:
        start_date = datetime(now.year, 1, 1).strftime('%Y-%m-%d')
        end_date = datetime(now.year, 12, 31).strftime('%Y-%m-%d')
        print("Matched 'this year':", [start_date, end_date])  # Debugging output
        return [start_date, end_date]

    if "last month" in query:
        if now.month == 1:  # Handle January (last month is December of the previous year)
            start_date = datetime(now.year - 1, 12, 1).strftime('%Y-%m-%d')
            end_date = datetime(now.year - 1, 12, 31).strftime('%Y-%m-%d')
        else:
            start_date = datetime(now.year, now.month - 1, 1).strftime('%Y-%m-%d')
            end_date = (datetime(now.year, now.month, 1) - timedelta(days=1)).strftime('%Y-%m-%d')
        print("Matched 'last month':", [start_date, end_date])
        return [start_date, end_date]

    if "this month" in query:
        start_date = datetime(now.year, now.month, 1).strftime('%Y-%m-%d')
        next_month = datetime(now.year, now.month, 28) + timedelta(days=4)
        end_date = (next_month - timedelta(days=next_month.day)).strftime('%Y-%m-%d')
        print("Matched 'this month':", [start_date, end_date])
        return [start_date, end_date]

    # Match date ranges like "from 01-01-2022 to 12-31-2022"
    range_match = re.search(r'from\s+([\w/-]+)\s+to\s+([\w/-]+)', query)
    if range_match:
        try:
            start_date = parse(range_match.group(1)).strftime('%Y-%m-%d')
            end_date = parse(range_match.group(2)).strftime('%Y-%m-%d')
            print("Matched range:", [start_date, end_date])  # Debugging output
            return [start_date, end_date]
        except Exception as e:
            print("Range parsing failed:", e)  # Debugging errors

    # Match explicit date patterns
    date_patterns = [
        r'\b\d{4}-\d{2}-\d{2}\b',  # ISO format: 2022-01-01
        r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b',  # Formats: 01/01/2022 or 01-01-22
        r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b'  # "January 1, 2022"
    ]

    for pattern in date_patterns:
        matches = re.findall(pattern, query)
        for match in matches:
            try:
                parsed_date = parse(match)  # No fuzzy argument
                dates.append(parsed_date.strftime('%Y-%m-%d'))
            except Exception as e:
                print("Date parsing failed for:", match, e)  # Debugging errors

    # Return sorted and deduplicated dates
    final_dates = sorted(set(dates))
    if len(final_dates) == 2:  # If two dates, assume it's a range
        print("Matched dates:", final_dates)
        return final_dates
    elif len(final_dates) == 1:  # Single date found
        print("Matched single date:", final_dates)
        return [final_dates[0], final_dates[0]]  # Treat as same start and end date
    else:
        print("No valid dates found.")
        return None



def extract_department_from_query(query, collection):
    """
    Extract department name dynamically from the query by searching in the MongoDB collection.
    """
    query = query.lower()

    departments = collection.distinct("Department Name")  # Assuming "Department Name" exists

    for department in departments:
        if department.lower() in query:
            print(f"DEBUG: Matched department: {department}")
            return department

    print("DEBUG: No department matched in query.")
    return None


def get_largest_order(collection):
    """
    Fetch the largest order based on the quantity of items.

    Args:
    - collection: MongoDB collection object.

    Returns:
    - Dict: Readable result containing the purchase order details and quantity, or an error message.
    """
    try:
        # MongoDB aggregation pipeline to find the order with the largest quantity
        pipeline = [
            {"$group": {"_id": "$Purchase Order Number", "Total Quantity": {"$sum": "$Quantity"}}},
            {"$sort": {"Total Quantity": -1}},  # Sort by total quantity in descending order
            {"$limit": 1}  # Limit to the top order
        ]
        result = list(collection.aggregate(pipeline))

        if result:
            # Fetch additional details for the largest order
            purchase_order_number = result[0]["_id"]
            total_quantity = result[0]["Total Quantity"]

            # Query to fetch order details
            order_details = collection.find_one({"Purchase Order Number": purchase_order_number}, {
                "Purchase Order Number": 1,
                "Department Name": 1,
                "Supplier Name": 1,
                "_id": 0
            })

            if order_details:
                # Format the result for readability
                return {
                    "Purchase Order Number": order_details.get("Purchase Order Number", "N/A"),
                    "Department Name": order_details.get("Department Name", "N/A"),
                    "Supplier Name": order_details.get("Supplier Name", "N/A"),
                    "Total Quantity": total_quantity
                }
            else:
                return {"Message": "Order details not found for the largest order."}
        else:
            return {"Message": "No orders found in the database."}
    except Exception as e:
        return {"Error": f"An error occurred while fetching the largest order: {str(e)}"}





def extract_category_from_query(query, collection):
    """
    Extract category name dynamically from the query by searching in the MongoDB collection.
    """
    query = query.lower()
    
    # Fetch categories from the database
    categories = collection.distinct("category_name")  # Assuming the collection has 'category_name' field
    print("Available categories:", categories)  # Debugging output
    
    # Search for category names in the user's query
    for category in categories:
        if category.lower() in query:
            return category
    return None

def extract_item_name_from_query(collection, query):
    """
    Extract the item name from the user's query by searching in MongoDB.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the item name.

    Returns:
    - str: The extracted item name, or None if not found.
    """
    query = query.lower()  # Convert to lowercase for case-insensitive matching

    # Use a regular expression to find matches for item names in the query
    # Searching for item names in the MongoDB database based on user's query
    result = collection.items.find({"item_name": {"$regex": query, "$options": "i"}}, {"item_name": 1, "_id": 0})

    # If we find any matching items, return the first one
    matching_items = list(result)
    if matching_items:
        return matching_items[0]["item_name"]
    
    # If no match is found, return None
    return None

def extract_acquisition_type_from_query(collection, query):

    
    """
    Extract the acquisition type from the user's query by searching in MongoDB.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the acquisition type.

    Returns:
    - str: The extracted acquisition type, or None if not found.
    """
    query = query.lower()  # Convert to lowercase for case-insensitive matching

    # Search MongoDB for acquisition types that match the user's query
    result = collection.acquisitions.find({"type": {"$regex": query, "$options": "i"}}, {"type": 1, "_id": 0})

    # If any acquisition types match, return the first one
    matching_acquisition_types = list(result)
    if matching_acquisition_types:
        # Return the first matching acquisition type
        return matching_acquisition_types[0]["type"]

    # If no match is found, return None
    return None

def extract_supplier_name_from_query(collection, query):
    """
    Extract the supplier name from the user's query by searching in MongoDB.

    Args:
    - collection: MongoDB collection object.
    - query (str): The user's query containing the supplier name.

    Returns:
    - str: The extracted supplier name, or None if not found.
    """
    query = query.lower()  # Convert query to lowercase for case-insensitive matching

    # Extract potential supplier name using the Supplier Name field
    suppliers = collection.distinct("Supplier Name")
    for supplier in suppliers:
        if supplier.lower() in query:
            return supplier  # Return the exact match from the database

    return None  # If no supplier name matches, return None


def extract_purchase_order_number_from_query(query):
    """
    Extract the purchase order number from the user's query.

    Args:
    - query (str): The user's query containing the purchase order number.

    Returns:
    - str: The extracted purchase order number, or None if not found.
    """
    # Define regex pattern for extracting purchase order number.
    # For example, it could be a format like "PO12345" or "12345" or similar patterns.
    po_number_pattern = r"(PO\d{5,})|\d{5,}"  # Matches 'PO' followed by digits or just digits with 5 or more digits

    query = query.lower()  # Convert query to lowercase for case-insensitive matching
    match = re.search(po_number_pattern, query)

    if match:
        return match.group(0)  # Return the matched purchase order number
    else:
        return None  # Return None if no purchase order number is found


def extract_fiscal_year_from_query(query):
    """
    Extract fiscal year from the user's query.
    
    Args:
    - query (str): The user's query containing the fiscal year.

    Returns:
    - str: The extracted fiscal year or fiscal year range (e.g., "2012-2014"), or None if not found.
    """
    query = query.lower()

    # Look for a 4-digit year (e.g., 2021, 2022, etc.)
    year_match = re.search(r'\b(20\d{2})\b', query)
    if year_match:
        return year_match.group(1)

    # Handle cases like "last year" or "next year"
    current_year = datetime.now().year
    
    if "last year" in query:
        return str(current_year - 1)
    
    if "next year" in query:
        return str(current_year + 1)

    # If nothing is found, return None
    return None

    
    return None  # Return None if no fiscal year found
# Example Usage
if __name__ == "__main__":
    connection_string = 'mongodb://localhost:27017/'
    collection = connect_to_mongodb(connection_string, 'purchases_large', 'purchases_dataset')
    print(get_total_quantity(collection))