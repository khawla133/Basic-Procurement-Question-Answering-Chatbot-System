# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 13:09:23 2024

@author: PRO
"""
import pandas as pd
from datasets import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
from sklearn.model_selection import train_test_split
#fine tune the model
# Load dataset
df = pd.read_csv("updated_balanced_procurement_intents.csv")

#fine tune the model
# Load dataset
df = pd.read_csv("/kaggle/input/new-procurement-data/updated_balanced_procurement_intents.csv")

# Map intents to numerical labels
unique_intents = df["intent"].unique()
intent_to_label = {intent: idx for idx, intent in enumerate(unique_intents)}
label_to_intent = {idx: intent for intent, idx in intent_to_label.items()}
df["label"] = df["intent"].map(intent_to_label)

# Split into training and test sets
train_texts, test_texts, train_labels, test_labels = train_test_split(
    df["user_input"], df["label"], test_size=0.2, random_state=42
)

# Convert to Hugging Face Dataset format
train_data = Dataset.from_dict({"text": train_texts, "label": train_labels})
test_data = Dataset.from_dict({"text": test_texts, "label": test_labels})

# Load tokenizer and model
model_name = "distilbert-base-uncased"
tokenizer = AutoTokenizer.from_pretrained(model_name)

def tokenize_function(example):
    return tokenizer(example["text"], truncation=True, padding="max_length", max_length=128)

# Tokenize datasets
train_data = train_data.map(tokenize_function, batched=True)
test_data = test_data.map(tokenize_function, batched=True)

# Define model
model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=len(unique_intents))

# Define training arguments
training_args = TrainingArguments(
    output_dir="./results",
    evaluation_strategy="epoch",
    learning_rate=2e-5,
    per_device_train_batch_size=16,
    per_device_eval_batch_size=16,
    num_train_epochs=55,
    weight_decay=0.01,
    logging_dir="./logs",
    logging_steps=10,
    save_strategy="epoch",
    save_total_limit=2,
    load_best_model_at_end=True
)

# Define Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_data,
    eval_dataset=test_data,
    tokenizer=tokenizer
)

# Train the model
trainer.train()

# Save the fine-tuned model
model.save_pretrained("procurement_intent_model")
tokenizer.save_pretrained("procurement_intent_model")

# Save label mapping
import json
with open("procurement_intent_model/label_mapping.json", "w") as f:
    json.dump(label_to_intent, f)

print("Model fine-tuned and saved!")

