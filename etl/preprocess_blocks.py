import os
import pandas as pd
import numpy as np

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_path = os.path.join(BASE_DIR, "data", "raw", "ethereum__blocks__21525890_to_21533057.csv")
output_path = os.path.join(BASE_DIR, "data", "intermediate", "ethereum__blocks__cleaned__21525890_to_21533057.csv")

# Load only the necessary columns
usecols = ["block_number", "chain_id", "block_hash", "timestamp", "parent_hash"]
df = pd.read_csv(input_path, usecols=usecols)

# Drop rows with missing essential values
df = df.dropna(subset=["block_number", "block_hash", "timestamp"])