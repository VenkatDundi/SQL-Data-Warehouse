import os
from fastapi import FastAPI
import json

app = FastAPI()             # Initializing the Fast API

Current_Directory = os.path.dirname(os.path.abspath(__file__))         # Capture Current Directory
File_Path = os.path.join(Current_Directory, "SubCategory.json")

# Load JSON
with open(File_Path, "r") as f:
    subcategories = json.load(f)

#print(subcategories)           # View the Json format of the Subcategories file

@app.get("/subcategories")      # get() to fetch the data
def get_subcategories():        # function that returns the subcategories
    return subcategories
