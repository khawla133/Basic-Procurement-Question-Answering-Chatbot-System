import logging
from flask import Flask, request, jsonify, session
from flask_cors import CORS
from transformers import pipeline
from pymongo import MongoClient
from query_functions import *  # Import the functions from your query_functions file
connection_string = 'mongodb://localhost:27017/'
client = MongoClient(connection_string)

# Access the database and collection
db = client['purchases_large']  
collection = db['purchases_dataset']  
app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = 'mysecret'

# Load the pre-trained model and intent mappings
model_path = 'procurement_intent_model'  # Path to your model directory
nlp_model = pipeline("text-classification", model=model_path, tokenizer=model_path)
import json

# Load label mapping from the model directory
with open(f"{model_path}/label_mapping.json", "r") as f:
    label_to_intent = json.load(f)

# Intent-function map
intent_map = {
    "show_highest_spending_quarter": get_highest_spending_quarter,
    "total_orders": get_total_orders,
    "frequent_items": get_frequent_line_items,
    "acquisition_spending": get_spending_by_acquisition_type,
    "total_quantity": get_total_quantity,
    "supplier_orders": get_orders_by_supplier,
    "acquisition_method_avg_price": get_acquisition_method_avg_price,
    "acquisition_method_department": get_acquisition_method_department,
    "acquisition_method_frequency": get_acquisition_method_frequency,
    "acquisition_method_spending": get_acquisition_method_spending,
    "acquisition_type_department_usage": get_acquisition_type_department_usage,
    "acquisition_type_orders": get_acquisition_type_orders,
    "acquisition_type_spending": get_acquisition_spending,
    "acquisition_type_top_suppliers": get_acquisition_type_top_suppliers,
    "avg_quantity_per_order": get_avg_quantity_per_order,
    "avg_unit_price_by_category": get_avg_unit_price_by_category,
    "bulk_items": get_bulk_items,
    "calcard_frequent_items": get_calcard_frequent_items,
    "calcard_orders": get_calcard_orders,
    "calcard_top_departments": get_calcard_top_departments,
    "calcard_total_spending": get_calcard_total_spending,
    "cheapest_item": get_cheapest_item,
    "classification_frequent_items": get_classification_frequent_items,
    "classification_items": get_classification_items,
    "classification_spending_breakdown": get_classification_spending_breakdown,
    "department_item_count": get_department_item_count,
    "department_spending_breakdown": get_department_spending_breakdown,
    "department_suppliers": get_department_suppliers,
    "department_top_purchases": get_department_top_purchases,
    "fiscal_year_expensive_item": get_fiscal_year_expensive_item,
    "fiscal_year_orders": get_fiscal_year_orders,
    "fiscal_year_spending": get_fiscal_year_spending,
    "fiscal_year_top_department": get_fiscal_year_top_department,
    "highest_total_price_order": get_highest_total_price_order,
    "item_details": get_item_details,
    "large_quantity_orders": get_large_quantity_orders,
    "purchase_order_details": get_purchase_order_details,
    "purchase_order_items": get_purchase_order_items,
    "purchase_order_supplier": get_purchase_order_supplier,
    "purchase_order_value": get_purchase_order_value,
    "quantity_top_department": get_quantity_top_department,
    "supplier_items": get_supplier_items,
    "supplier_spending": get_supplier_spending,
    "supplier_top_orders": get_supplier_top_orders,
    "supplier_top_revenue": get_supplier_top_revenue,
    "top_classification_code": get_top_classification_code,
    "total_price_by_category": get_total_price_by_category,
    "total_price_by_quarter": get_total_price_by_quarter,
    "unit_price_item": get_unit_price_item,
    "greeting": handle_greeting,
    "department_spending_by_name": get_department_spending_by_name,
    "frequent_line_items": get_frequent_line_items,
    "highest_spending_department": get_highest_spending_department,
    "largest_order": get_largest_order,
    "department_spending": get_department_spending_breakdown
}

# Function to detect the intent from user input
def detect_intent(user_input):
    result = nlp_model(user_input)
    label_index = result[0]["label"].replace("LABEL_", "")
    
    # Map the label index to the intent name
    intent = label_to_intent.get(label_index)
    if intent is None:
        print(f"Unrecognized intent label: {label_index}")
        return None  # Return None if the intent is not recognized
    return intent
def generate_response(intent, result, extracted_dates=None):
    """
    Generate a user-friendly response based on the intent and result.

    Args:
    - intent (str): Detected intent.
    - result (Any): Result from the corresponding intent handler.
    - extracted_dates (List[str], optional): Dates extracted from the query.

    Returns:
    - str: Generated response for the user.
    """
    if intent == "greeting":
        return "Hi, how may I assist you today?"

    elif intent == "total_orders":
        if extracted_dates:
            start_date, end_date = extracted_dates
            return f"There were {result} orders placed between {start_date} and {end_date}."
        return f"There were {result} orders placed."

    elif intent == "show_highest_spending_quarter":
        if result:
            return f"The highest spending quarter is {result['_id']} with a total spending of ${result['total_spending']:,}."
        return "Sorry, no data was found for the highest spending quarter."

    elif intent == "frequent_items":
        if isinstance(result, list) and result:
            items = ", ".join([f"{item['item_name']} ({item['frequency']} times)" for item in result])
            return f"The most frequent items are: {items}."
        return "No frequent items were found."

    elif intent == "acquisition_spending":
        if isinstance(result, list) and result:
            spending = "\n".join([f"{item['acquisition_type']}: ${item['total_spending']:,}" for item in result])
            return f"Spending by acquisition type:\n{spending}"
        return "No data found for acquisition spending."

    elif intent == "total_quantity":
    # If the result is already a preformatted string, return it directly
        if isinstance(result, str):
            return result
        elif isinstance(result, (int, float)) and result > 0:
            # If the result is a number, format and return it
            return f"The total quantity of items ordered is {result:,}."
        return "The total quantity of items ordered could not be determined."

    elif intent == "supplier_orders":
        if result and isinstance(result, list):
            readable_orders = "\n".join([
                f"- Purchase Order: {order.get('Purchase Order Number', 'N/A')}, "
                f"Total Price: {order.get('Total Price', 'N/A')}, "
                f"Date: {order.get('Creation Date', 'N/A')}"
                for order in result
            ])
            return f"Orders for the supplier:\n{readable_orders}"
        return "No orders were found for the specified supplier."


    elif intent == "acquisition_method_avg_price":
        if isinstance(result, list) and result:
            formatted_methods = "\n".join(
                [f"{item['_id']}: ${item['avg_price']:,}" for item in result]
            )
            return f"The average prices for acquisition methods are:\n{formatted_methods}"
        return "No data available for acquisition methods' average prices."
    

    elif intent == "calcard_total_spending":
        return f"The total spending using CalCard is ${result:,.2f}."

    elif intent == "classification_spending_breakdown":
        if isinstance(result, list) and result:
            breakdown = "\n".join([f"Code: {item['classification_code']}, Spending: ${item['total_spending']:,}" for item in result])
            return f"Spending breakdown by classification:\n{breakdown}"
        return "No spending breakdown found for classifications."

    elif intent == "expensive_items":
        if isinstance(result, list) and result:
            items = ", ".join([f"{item['item_name']} (${item['unit_price']:,})" for item in result])
            return f"The most expensive items are: {items}."
        return "No expensive items were found."

    elif intent == "fiscal_year_spending":
        if isinstance(result, list) and len(result) > 0:  # Ensure result is a non-empty list
            total_spending = result[0].get("Total Spending", 0)  # Access the first element's "Total Spending"
            return f"The total spending for the fiscal year is ${total_spending:,.2f}."
        return "No spending data found for the specified fiscal year."


    elif intent == "purchase_order_details":
        if isinstance(result, dict):
            details = "\n".join([f"{key}: {value}" for key, value in result.items()])
            return f"Details of the purchase order:\n{details}"
        return "No details found for the specified purchase order."

    elif intent == "unit_price_item":
        return f"The unit price for the specified item is ${result:,.2f}."

    elif intent == "supplier_top_revenue":
        if isinstance(result, dict):
            return f"The supplier with the highest revenue is {result['supplier_name']} with a total revenue of ${result['total_revenue']:,}."
        return "No data found for supplier revenue."

    elif intent == "top_classification_code":
        if result:
            return f"The classification code with the highest spending is {result['_id']} with a total spending of ${result['total_spending']:,}."
        return "No classification code data found."

    elif intent == "largest_order":
        if isinstance(result, dict):
            return f"The largest order placed was {result['order_id']} with a quantity of {result['quantity']} items."
        return "No data for the largest order was found."
    if intent == "get_acquisition_method_department":
        if isinstance(result, list) and result:
            # Construct a detailed response
            response_lines = [
                f"Acquisition Method: {entry['Acquisition Method']} is used by the following departments: {', '.join(entry['Departments'])}."
                for entry in result
            ]
            return "\n".join(response_lines)
        return "No data was found for acquisition methods and their associated departments."
    if intent == "cheapest_item":
        if result:
            return (
                f"The cheapest item is '{result['Item Name']}' priced at ${result['Unit Price']:.2f}.\n"
                f"Supplier: {result['Supplier Name']}\n"
                f"Department: {result['Department Name']}\n"
                f"Purchase Order: {result['Purchase Order Number']}\n"
                f"Description: {result['Description']}"
            )
        return "No data found for the cheapest item."
    if intent == "get_acquisition_spending":
        if result:
            response = "\n".join([f"{item['Acquisition Type']}: ${item['Total Spending']:,}" for item in result])
            return f"Total spending by acquisition type:\n{response}"
        return "No spending data found for acquisition types."

    elif intent == "acquisition_type_department_usage":
        if result:
            response = "\n".join([
                f"Acquisition Type: {item['Acquisition Type']}, Department: {item['Department']}, Total Spending: ${item['Total Spending']:,}"
                for item in result
            ])
            return f"Department usage by acquisition type:\n{response}"
        return "No department usage data found for acquisition types."

    elif intent == "acquisition_type_orders":
        if result:
            response = "\n".join([f"{item['Acquisition Type']}: {item['Total Orders']} orders" for item in result])
            return f"Total orders by acquisition type:\n{response}"
        return "No orders data found for acquisition types."

    elif intent == "acquisition_type_top_suppliers":
        if result:
            response = "\n".join([
                f"Acquisition Type: {item['Acquisition Type']}, Supplier: {item['Supplier']}, Total Spending: ${item['Total Spending']:,}"
                for item in result
            ])
            return f"Top suppliers by acquisition type:\n{response}"
        return "No top suppliers data found for acquisition types."

    elif intent == "avg_quantity_per_order":
        if result:
            return f"The average quantity per order is {result['Average Quantity Per Order']}."
        return "No data available for average quantity per order."

    elif intent == "get_avg_unit_price_by_category":
        if result:
            response = "\n".join([
                f"Classification Code: {item['Classification Code']}, Average Unit Price: ${item['Average Unit Price']:,}"
                for item in result
            ])
            return f"Average unit price by category:\n{response}"
        return "No unit price data found for categories."

    elif intent == "bulk_items":
        if result:
            response = "\n".join([f"Item Name: {item['Item Name']}, Quantity: {item['Quantity']}" for item in result])
            return f"Items purchased in bulk:\n{response}"
        return "No bulk item data found."

    elif intent == "calcard_frequent_items":
        if result:
            response = "\n".join([f"Item Name: {item['Item Name']}, Frequency: {item['Frequency']}" for item in result])
            return f"Most frequent items purchased using CalCard:\n{response}"
        return "No frequent items data found for CalCard."

    elif intent == "calcard_orders":
        if result:
            response = "\n".join([f"CalCard: {item['CalCard']}, Total Orders: {item['Total Orders']}" for item in result])
            return f"Total orders by CalCard usage:\n{response}"
        return "No orders data found for CalCard."

    elif intent == "calcard_top_departments":
        if result:
            response = "\n".join([
                f"CalCard: {item['CalCard']}, Department: {item['Department']}, Total Spending: ${item['Total Spending']:,}"
                for item in result
            ])
            return f"Top departments using CalCard:\n{response}"
        return "No top departments data found for CalCard."

    elif intent == "calcard_total_spending":
        if result:
            response = "\n".join([f"CalCard: {item['CalCard']}, Total Spending: ${item['Total Spending']:,}" for item in result])
            return f"Total spending by CalCard:\n{response}"
        return "No spending data found for CalCard."
    
    
    if intent == "highest_total_price_order":
        if result:
            order = result[0]
            return f"The highest total price order is '{order['Purchase Order Number']}' with a total price of ${order['Total Price']:,}."
        return "No data found for the highest total price order."

    elif intent == "large_quantity_orders":
        if result:
            orders = "\n".join([
                f"Item: {order['Item Name']}, Quantity: {order['Quantity']}, Order Number: {order['Purchase Order Number']}"
                for order in result
            ])
            return f"Orders with large quantities:\n{orders}"
        return "No large quantity orders were found."

    elif intent == "total_price_by_category":
        if result:
            categories = "\n".join([
                f"Classification Code: {category['Classification Code']}, Total Price: ${category['Total Price']:,}"
                for category in result
            ])
            return f"Total price by category:\n{categories}"
        return "No data found for total price by category."

    elif intent == "total_price_by_quarter":
        if result:
            quarters = "\n".join([
                f"{quarter['Quarter']}: ${quarter['Total Price']:,}"
                for quarter in result
            ])
            return f"Total price by quarter:\n{quarters}"
        return "No data found for total price by quarter."

    elif intent == "classification_frequent_items":
        if result:
            items = "\n".join([
                f"Classification Code: {item['Classification Code']}, Frequency: {item['Frequency']}"
                for item in result
            ])
            return f"Most frequent classification items:\n{items}"
        return "No frequent classification items were found."

    elif intent == "get_classification_items":
        if result:
            items = "\n".join([
                f"Classification Code: {item['Classification Code']}, Item: {item['Item Name']}, Total Quantity: {item['Total Quantity']}"
                for item in result
            ])
            return f"Items by classification code:\n{items}"
        return "No items found for the specified classification code."

    elif intent == "classification_spending_breakdown":
        if result:
            spending = "\n".join([
                f"Classification Code: {item['Classification Code']}, Total Spending: ${item['Total Spending']:,}"
                for item in result
            ])
            return f"Spending breakdown by classification code:\n{spending}"
        return "No spending data found for classification codes."

    elif intent == "department_item_count":
        if result:
            departments = "\n".join([
                f"Department: {dept['Department Name']}, Total Item Count: {dept['Total Item Count']}"
                for dept in result
            ])
            return f"Item count by department:\n{departments}"
        return "No item count data found for departments."

    elif intent == "department_spending_breakdown" or "department_spending":
        if result:
            spending = "\n".join([
                f"Department: {dept['Department Name']}, Total Spending: {dept['Total Spending']}"
                for dept in result
            ])
            return f"Spending breakdown by department:\n{spending}"
        return "No spending data found for departments."

    elif intent == "department_top_purchases":
        if result and "Message" not in result[0]:
            purchases = "\n".join([
                f"Item: {purchase['Item Name']}, Total Spending: {purchase['Total Spending']}"
                for purchase in result
            ])
            return f"Top purchases for the department:\n{purchases}"
        return result[0]["Message"]

    elif intent == "fiscal_year_spending":
        if result and "Message" not in result[0]:
            return f"Total spending for fiscal year {result[0]['Fiscal Year']}: {result[0]['Total Spending']}."
        return result[0]["Message"]

    elif intent == "get_fiscal_year_top_department":
        if result and "Message" not in result[0]:
            return f"The top department for fiscal year is {result[0]['Department Name']} with total spending of {result[0]['Total Spending']}."
        return result[0]["Message"]
    
    if intent == "fiscal_year_expensive_item":
        if not result:
            return "No data available for the specified fiscal year."

        if "Message" in result[0]:
            return result[0]["Message"]

        item = result[0]
        return (
            f"The most expensive item in the specified fiscal year is '{item['Item Name']}' "
            f"with a unit price of {item['Unit Price']}. "
            f"It was purchased under the order number '{item['Purchase Order Number']}' "
            f"by the '{item['Department Name']}' department.")
    

    elif intent == "supplier_spending":
        if result and "Message" not in result[0]:
            return f"Total spending for supplier {result[0]['Supplier Name']}: {result[0]['Total Spending']}."
        return result[0]["Message"]

    elif intent == "supplier_top_orders":
        if result and "Message" not in result[0]:
            orders = "\n".join([
                f"Order Number: {order['Purchase Order Number']}, Order Value: {order['Order Value']}"
                for order in result
            ])
            return f"Top orders for the supplier:\n{orders}"
        return result[0]["Message"]

    
    elif intent == "spending_by_acquisition_type":
        if result and "Message" not in result[0]:
            spending = "\n".join([
                f"Acquisition Type: {item['Acquisition Type']}, Total Spending: {item['Total Spending']}"
                for item in result
            ])
            return f"Spending by acquisition type:\n{spending}"
        return result[0]["Message"]
    
    elif intent == "department_suppliers":
        if isinstance(result, list) and result:  # Ensure result is a non-empty list
            suppliers = "\n".join(
                [f"- {supplier.get('Supplier Name', 'N/A')}" for supplier in result]
            )
            return f"The suppliers for the department are:\n{suppliers}"
        elif isinstance(result, dict) and "Message" in result:  # Handle error messages
            return result["Message"]
        else:
            return "No suppliers were found for the specified department."

        
    elif intent == "fiscal_year_top_department":
        if isinstance(result, list) and result:
            department = result[0]["Department Name"]
            total_spending = result[0]["Total Spending"]
            return f"The top department in the fiscal year is '{department}' with a total spending of {total_spending}."
        elif isinstance(result, dict) and "message" in result:
            return result["message"]
        else:
            return "No data found for the top department in the specified fiscal year."
    
    if intent == "supplier_spending":
        if isinstance(result, list) and result:
            supplier_name = result[0]["Supplier Name"]
            total_spending = result[0]["Total Spending"]
            return f"The total spending for supplier '{supplier_name}' is {total_spending}."
        elif isinstance(result, dict) and "message" in result:
            return result["message"]
        else:
            return "No spending data found for the specified supplier."

    elif intent == "supplier_top_orders":
        if isinstance(result, list) and result:
            orders = "\n".join(
                [f"Order ID: {item['Purchase Order Number']}, Total Value: {item['Order Value']}" for item in result]
            )
            return f"The top orders for the specified supplier are:\n{orders}"
        elif isinstance(result, dict) and "message" in result:
            return result["message"]
        else:
            return "No order data found for the specified supplier."


    elif intent == "spending_by_acquisition_type":
        if isinstance(result, list) and result:
            spending = "\n".join(
                [f"Acquisition Type: {item['Acquisition Type']}, Total Spending: {item['Total Spending']}" for item in result]
            )
            return f"The spending breakdown by acquisition type is:\n{spending}"
        elif isinstance(result, dict) and "message" in result:
            return result["message"]
        else:
            return "No spending data found for the specified acquisition type."
    elif  intent == "fiscal_year_orders":
        if isinstance(result, list) and result:
            return result[0].get("Message", "I couldn't find any relevant data.")
        return "I couldn't process your query for fiscal year orders. Please try again."
    elif intent == "high_unit_price_items":
        if isinstance(result, list) and result:  # Check if the result is a non-empty list
            response_lines = [
                f"- Item Name: {str(item.get('Item Name', 'N/A'))}, Unit Price: {str(item.get('Unit Price', 'N/A'))}, Purchase Order Number: {str(item.get('Purchase Order Number', 'N/A'))}"
                for item in result
            ]
            response = "The items with high unit prices are:\n"
            response += "\n".join(response_lines)
            return response

        return "No items with high unit prices were found."
    
    
    if intent == "largest_order":
        return (
            f"The largest order is:\n"
            f"- Purchase Order Number: {result.get('Purchase Order Number', 'N/A')}\n"
            f"- Department Name: {result.get('Department Name', 'N/A')}\n"
            f"- Supplier Name: {result.get('Supplier Name', 'N/A')}\n"
            f"- Total Quantity: {result.get('Total Quantity', 'N/A')}"
        )

    elif intent == "highest_spending_department":
        return (
            f"The highest spending department is '{result.get('Department Name', 'N/A')}' "
            f"with a total spending of ${result.get('Total Spending', 'N/A')}."
        )

    elif intent == "department_spending_by_name":
        if isinstance(result, dict):  # Ensure the result is a dictionary
            if "Department Name" in result and "Total Spending" in result:
                # Extract values safely
                department_name = result.get("Department Name", "Unknown Department")
                total_spending = result.get("Total Spending", "Unknown Spending")
                return (
                    f"The total spending for the department '{department_name}' is {total_spending}."
                )
            elif "Message" in result:  # Handle error messages
                return result["Message"]
            elif "Error" in result:  # Handle unexpected errors
                return result["Error"]
        return "No spending data could be determined."




    elif intent == "top_classification_code":
        return (
            f"The classification code with the highest total spending is "
            f"{result.get('Classification Code', 'N/A')} "
            f"with a total spending of ${result.get('Total Spending', 'N/A')}."
        )

    elif intent == "quantity_top_department":
        return (
            f"The department with the highest total quantity is '{result.get('Department Name', 'N/A')}' "
            f"with a total quantity of {result.get('Total Quantity', 'N/A')}."
        )

    elif intent == "unit_price_item":
        return (
            f"Details for the item '{result.get('Item Name', 'N/A')}':\n"
            f"- Unit Price: ${result.get('Unit Price', 'N/A')}\n"
            f"- Purchase Order Number: {result.get('Purchase Order Number', 'N/A')}\n"
            f"- Department Name: {result.get('Department Name', 'N/A')}\n"
            f"- Supplier Name: {result.get('Supplier Name', 'N/A')}"
        )

    elif intent == "fiscal_year_orders":
        return (
            f"The total number of orders placed in fiscal year {result.get('Fiscal Year', 'N/A')} "
            f"is {result.get('Total Orders', 'N/A')}."
        )

    elif intent == "fiscal_year_expensive_item":
        return (
            f"The most expensive item in fiscal year {result.get('Fiscal Year', 'N/A')} is:\n"
            f"- Item Name: {result.get('Item Name', 'N/A')}\n"
            f"- Unit Price: ${result.get('Unit Price', 'N/A')}\n"
            f"- Purchase Order Number: {result.get('Purchase Order Number', 'N/A')}"
        )

    elif intent == "fiscal_year_top_department":
        return (
            f"The department with the highest spending in fiscal year {result.get('Fiscal Year', 'N/A')} is:\n"
            f"- Department Name: {result.get('Department Name', 'N/A')}\n"
            f"- Total Spending: ${result.get('Total Spending', 'N/A')}"
        )

    elif intent == "department_top_purchases":
        if isinstance(result, list):
            purchases = "\n".join(
                [
                    f"- Item Name: {purchase.get('Item Name', 'N/A')}, Total Spending: ${purchase.get('Total Spending', 'N/A')}"
                    for purchase in result
                ]
            )
            return f"The top purchases for the department are:\n{purchases}"
        return "No purchases were found for the specified department."

    elif intent == "high_unit_price_items":
        if isinstance(result, list) and result:  # Check if the result is a non-empty list
            response_lines = [
                f"- Item Name: {item.get('Item Name') or item.get('item_name', 'N/A')}, "
                f"Unit Price: {item.get('Unit Price') or item.get('unit_price', 'N/A')}, "
                f"Purchase Order Number: {item.get('Purchase Order Number') or item.get('purchase_order_number', 'N/A')}"
                for item in result
            ]
            response = "The items with high unit prices are:\n"
            response += "\n".join(response_lines)
            return response
        return "No items with high unit prices were found."


    elif intent == "supplier_spending":
        return (
            f"The total spending for supplier '{result.get('Supplier Name', 'N/A')}' is "
            f"${result.get('Total Spending', 'N/A')}."
        )

    elif intent == "supplier_top_orders":
        if isinstance(result, list):
            orders = "\n".join(
                [
                    f"- Purchase Order Number: {order.get('Purchase Order Number', 'N/A')}, "
                    f"Order Value: ${order.get('Order Value', 'N/A')}"
                    for order in result
                ]
            )
            return f"The top orders for the supplier are:\n{orders}"
        return "No top orders were found for the specified supplier."

    elif intent == "get_spending_by_acquisition_type":
        if isinstance(result, list):
            spending = "\n".join(
                [
                    f"- Acquisition Type: {entry.get('Acquisition Type', 'N/A')}, "
                    f"Total Spending: ${entry.get('Total Spending', 'N/A')}"
                    for entry in result
                ]
            )
            return f"The spending by acquisition type is:\n{spending}"
        return "No spending data found for the specified acquisition type."
    
    else:
        return "I'm sorry, I couldn't process your request. Could you please try again or rephrase your query?"
    

@app.route('/chat', methods=['POST'])
def chatbot():
    data = request.json
    user_input = data.get("message", "")

    if not user_input:
        return jsonify({"success": False, "message": "Input message is missing."})

    try:
        # Detect intent
        intent = detect_intent(user_input)
        print(f"Detected intent: {intent}")  # Debugging

        # Check if the intent is in the intent_map
        if intent not in intent_map:
            return jsonify({"success": False, "message": "Intent not recognized."})

        # Initialize result variable
        result = None

        # Handle specific intents with required parameters
        if intent == "total_orders":
            extracted_dates = extract_dates_from_query(user_input)
            if extracted_dates and len(extracted_dates) == 2:
                start_date, end_date = extracted_dates
                result = intent_map[intent](collection, start_date, end_date)
            else:
                return jsonify({"success": False, "message": "Date range not found in query."})

        elif intent == "department_spending_by_name":
            department_name = extract_department_from_query(user_input,collection)
            if department_name:
                result = intent_map[intent](collection, department_name)
            else:
                return jsonify({"success": False, "message": "Department name not found in query."})

        elif intent == "fiscal_year_spending":
            fiscal_year = extract_fiscal_year_from_query(user_input)
            if fiscal_year:
                result = intent_map[intent](collection, fiscal_year)
            else:
                return jsonify({"success": False, "message": "Fiscal year not found in query."})

        elif intent == "fiscal_year_orders":
            fiscal_year = extract_fiscal_year_from_query(user_input)
            if fiscal_year:
                result = intent_map[intent](collection, fiscal_year)
            else:
                return jsonify({"success": False, "message": "Fiscal year not found in query."})

        elif intent == "supplier_orders":
            supplier_name = extract_supplier_name_from_query(collection, user_input)
            if supplier_name:
                result = intent_map[intent](collection, supplier_name)
            else:
                return jsonify({"success": False, "message": "Supplier name not found in query."})
        elif intent == "department_suppliers":
            department_name = extract_department_from_query(user_input, collection)
            print(f"DEBUG: Extracted department name: {department_name}")
            if department_name:
                result = intent_map[intent](collection, department_name)
                print(f"DEBUG: Result from department_suppliers intent: {result}")
            else:
                return jsonify({"success": False, "message": "Department name not found in query."})
        
            if not result:
                return jsonify({"success": False, "message": "No suppliers were found for the specified department."})
        
            response_message = generate_response(intent, result)
            return jsonify({"success": True, "message": response_message, "data": result})

        elif intent == "fiscal_year_expensive_item":
            fiscal_year = extract_fiscal_year_from_query(user_input)
            if fiscal_year:
                result = intent_map[intent](collection, fiscal_year)
            else:
                return jsonify({"success": False, "message": "Fiscal year not found in query."})

        # Handle generic intents without parameters
        else:
            result = intent_map[intent](collection)

        # Check if result is None or empty
        if not result:
            return jsonify({"success": False, "message": "No data found for the query."})

        # Generate a response
        response_message = generate_response(intent, result)

        # Return a valid response
        return jsonify({"success": True, "message": response_message, "data": result})


    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return jsonify({"success": False, "message": f"An error occurred: {str(e)}"})



if __name__ == "__main__":
    app.run(debug=True)
