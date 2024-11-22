import streamlit as st
import requests
import pandas as pd

# Initialize session state for storing messages
if "messages" not in st.session_state:
    st.session_state["messages"] = [{"role": "bot", "content": "Hi, how may I assist you today?"}]

st.title("Procurement Chat Assistant")

# Chat display
st.write("### Chat History")
for message in st.session_state["messages"]:
    if message["role"] == "bot":
        st.markdown(f"**🤖 Bot:** {message['content']}")
    else:
        st.markdown(f"**🧑 You:** {message['content']}")

# User input section
st.write("### Your Input")
user_query = st.text_input("Your message:", placeholder="Type your message here...")

# Send button to handle user query
if st.button("Send"):
    if user_query.strip():  # Ensure the input is not empty
        with st.spinner("Processing..."):
            try:
                # Call the Flask backend
                response = requests.post("http://127.0.0.1:5000/chat", json={"message": user_query})
                if response.status_code == 200:
                    response_data = response.json()

                    # Extract and handle the bot's reply
                    if response_data.get("success"):
                        bot_reply = response_data.get("message", "The assistant responded, but no message was found.")

                        # Check if the response contains structured tabular data
                        if "data" in response_data and isinstance(response_data["data"], list):
                            st.write("### Tabular Response")
                            df = pd.DataFrame(response_data["data"])
                            st.table(df)  # Display the tabular data as a table
                        else:
                            # Append the user's query and bot's response to the chat history
                            st.session_state["messages"].append({"role": "user", "content": user_query})
                            st.session_state["messages"].append({"role": "bot", "content": bot_reply})
                            st.write(f"**🤖 Bot:** {bot_reply}")
                    else:
                        st.error(response_data.get("message", "An error occurred while processing your request."))
                else:
                    st.error(f"Server error: {response.status_code}. Please try again later.")
            except requests.exceptions.RequestException as e:
                st.error(f"Error communicating with the server: {e}")
    else:
        st.warning("Please enter a message before sending.")

# Clear chat history button
if st.button("Clear Chat"):
    st.session_state["messages"] = [{"role": "bot", "content": "Hi, how may I assist you today?"}]
    st.success("Chat history cleared!")
