import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from transformers import pipeline
from pymongo import MongoClient
import json
from query_functions import *  # Ensure all required functions are defined here

# Database setup
connection_string = 'mongodb://localhost:27017/'
client = MongoClient(connection_string)
db = client['purchases_large']  
collection = db['purchases_dataset']  

# Flask app initialization
app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = 'mysecret'

# Model setup
model_path = 'procurement_intent_model'  # Path to your model directory
nlp_model = pipeline("text-classification", model=model_path, tokenizer=model_path)

# Load intent mappings
with open(f"{model_path}/label_mapping.json", "r") as f:
    label_to_intent = json.load(f)

# Intent-to-function mapping
intent_map = {
    "show_highest_spending_quarter": get_highest_spending_quarter,
    "total_orders": get_total_orders,
    "frequent_items": get_frequent_line_items,
    "acquisition_spending": get_spending_by_acquisition_type,
    "total_quantity": get_total_quantity,
    "supplier_orders": get_orders_by_supplier,
    "department_suppliers": get_department_suppliers,
    "fiscal_year_spending": get_fiscal_year_spending,
    "department_spending_by_name": get_department_spending_by_name,
    "highest_spending_department": get_highest_spending_department,
    "fiscal_year_orders": get_fiscal_year_orders,
    "department_spending_breakdown": get_department_spending_breakdown,
    "frequent_line_items": get_frequent_line_items,
    # Add additional mappings here as needed
}

# Intent detection function
def detect_intent(user_input):
    try:
        result = nlp_model(user_input)
        label_index = result[0]["label"].replace("LABEL_", "")
        return label_to_intent.get(label_index)
    except Exception as e:
        logging.error(f"Error detecting intent: {e}")
        return None

# Generate response function
def generate_response(intent, result):
    try:
        if intent == "show_highest_spending_quarter":
            if result:
                return (
                    f"The highest spending quarter is '{result['_id']}' with a total spending of "
                    f"${result['total_spending']:,.2f}."
                )
            return "No data was found for the highest spending quarter."

        elif intent == "total_orders":
            if result > 0:
                return f"The total number of orders placed is {result}."
            else:
                return "There were no orders placed in the specified date range."


        elif intent == "frequent_line_items":
            if isinstance(result, list) and result:
                items = "\n".join([f"- {item['Item Name']} ({item['Frequency']} times)" for item in result])
                return f"The most frequently ordered items are:\n{items}"
            return "No frequent items were found."

        elif intent == "acquisition_spending":
            if isinstance(result, list) and result:
                spending = "\n".join(
                    [f"- Acquisition Type: {item['Acquisition Type']}, Spending: ${item['Total Spending']:,.2f}" for item in result]
                )
                return f"Spending by acquisition type:\n{spending}"
            return "No spending data available for acquisition types."

        elif intent == "total_quantity":
            try:
                # Debugging: Log the result
                logging.debug(f"DEBUG: Received result for 'total_quantity': {result}")
        
                # Ensure result is a dictionary and has the expected keys
                if isinstance(result, dict) and result.get("success"):
                    total_quantity = result.get("total_quantity", 0)
                    return f"The total quantity of items ordered is {total_quantity:,}."
                elif isinstance(result, dict) and not result.get("success"):
                    return result.get("message", "An error occurred while retrieving total quantity data.")
                else:
                    return "Unexpected data structure received for total quantity."
            except Exception as e:
                # Log the error for debugging
                logging.error(f"Error in generate_response for intent 'total_quantity': {str(e)}")
                return "An error occurred while generating the response for total quantity."



        elif intent == "supplier_orders":
            if isinstance(result, list) and result:
                orders = "\n".join([
                    f"- Purchase Order: {order.get('Purchase Order Number', 'N/A')}, "
                    f"Total Price: ${order.get('Total Price', 'N/A')}, "
                    f"Date: {order.get('Creation Date', 'N/A')}"
                    for order in result
                ])
                return f"Orders for the supplier:\n{orders}"
            return "No orders were found for the specified supplier."

        elif intent == "department_suppliers":
            if isinstance(result, list) and result:
                suppliers = "\n".join([f"- {supplier['Supplier Name']}" for supplier in result])
                return f"Suppliers for the department are:\n{suppliers}"
            return "No suppliers were found for the specified department."

        elif intent == "fiscal_year_spending":
            if isinstance(result, list) and result:
                fiscal_year = result[0].get("Fiscal Year", "N/A")
                total_spending = result[0].get("Total Spending", "N/A")
                return f"Total spending for fiscal year {fiscal_year}: ${total_spending}."
            return "No spending data found for the specified fiscal year."

        elif intent == "department_spending_by_name":
            if isinstance(result, dict) and "Department Name" in result:
                return f"The total spending for the department '{result['Department Name']}' is {result['Total Spending']}."
            elif "Message" in result:
                return result["Message"]
            return "No spending data could be determined."

        elif intent == "highest_spending_department":
            if isinstance(result, dict) and "Department Name" in result:
                return f"The highest spending department is '{result['Department Name']}' with a total spending of {result['Total Spending']}."
            return "No data found for the highest spending department."

        elif intent == "fiscal_year_orders":
            if isinstance(result, list) and result:
                fiscal_year = result[0].get("Fiscal Year", "N/A")
                total_orders = result[0].get("Total Orders", "N/A")
                return f"The total number of orders in fiscal year {fiscal_year} is {total_orders}."
            return "No orders data found for the specified fiscal year."
        
        elif intent == "department_spending_breakdown" :
            if result:
                spending = "\n".join([
                    f"Department: {dept['Department Name']}, Total Spending: {dept['Total Spending']}"
                    for dept in result
                ])
                return f"Spending breakdown by department:\n{spending}"
            return "No spending data found for departments."

        return "I'm sorry, I couldn't process your request. Please try again."
    except Exception as e:
        logging.error(f"Error generating response: {e}")
        return "An error occurred while generating the response."


# Chat endpoint
@app.route('/chat', methods=['POST'])
def chatbot():
    data = request.json
    user_input = data.get("message", "")

    if not user_input:
        return jsonify({"success": False, "message": "Input message is missing."})

    try:
        # Detect intent
        intent = detect_intent(user_input)
        if not intent or intent not in intent_map:
            return jsonify({"success": False, "message": "Intent not recognized."})

        # Execute the corresponding function
        result = None
        if intent in ["department_spending_by_name", "department_suppliers"]:
            parameter = extract_department_from_query(user_input, collection)
            if parameter:
                result = intent_map[intent](collection, parameter)
            else:
                return jsonify({"success": False, "message": "Relevant parameter not found in query."})
        elif intent in ["fiscal_year_spending", "fiscal_year_orders"]:
            fiscal_year = extract_fiscal_year_from_query(user_input)
            if fiscal_year:
                result = intent_map[intent](collection, fiscal_year)
            else:
                return jsonify({"success": False, "message": "Fiscal year not found in query."})
        else:
            result = intent_map[intent](collection)

        # Handle empty results
        if not result:
            return jsonify({"success": False, "message": "No data found for the query."})

        # Generate response
        response_message = generate_response(intent, result)
        return jsonify({"success": True, "message": response_message, "data": result})

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return jsonify({"success": False, "message": f"An error occurred: {str(e)}"})

if __name__ == "__main__":
    app.run(debug=True)
