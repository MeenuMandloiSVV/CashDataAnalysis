import streamlit as st
import pymongo
import pandas as pd

class MongoDBProcessor:
    def __init__(self, mongo_uri, db_name):
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[db_name]

        self.collections = {
            "Index Swings": "indexswings",
            "Close to Close": "closetoclose",
            "Monthly High": "monthonmonth"
        }

        # Initialize empty data variables
        self.indexswingsdata = None
        self.CaseClosetoClosedata = None
        self.month_high_data = None

    def read_mongo_collection(self, collection_name):
        """Read entire MongoDB collection without limit."""
        try:
            collection = self.db[self.collections[collection_name]]
            data = list(collection.find({}, {"_id": 0}))  # ⚡ No limit, fetch all rows

            df = pd.DataFrame(data)

            if df.empty:
                st.warning(f"No data found in {collection_name} collection.")
            return df

        except Exception as e:
            st.error(f"Failed to read {collection_name} collection: {e}")
            st.text(traceback.format_exc())  
            return pd.DataFrame()

    def load_data(self):
        """Load all MongoDB collections into DataFrames."""
        self.indexswingsdata = self.read_mongo_collection("Index Swings")
        self.CaseClosetoClosedata = self.read_mongo_collection("Close to Close")
        self.month_high_data = self.read_mongo_collection("Monthly High")
