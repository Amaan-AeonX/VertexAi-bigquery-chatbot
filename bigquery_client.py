import os
from google.cloud import bigquery
from typing import List, Dict, Any
import pandas as pd

GOOGLE_CLOUD_PROJECT="raymond-maini-iiot"
BIGQUERY_DATASET="cnc_dataset"

class BigQueryClient:
    def __init__(self):
        self.project_id = GOOGLE_CLOUD_PROJECT
        self.client = bigquery.Client(project=self.project_id)
        self.datasets = [BIGQUERY_DATASET, "dev_public"]
    
    def get_all_tables(self) -> Dict[str, List[str]]:
        """Get all tables (excluding views) from both datasets"""
        all_tables = {}
        
        for dataset_id in self.datasets:
            if not dataset_id:
                continue
            tables = []
            dataset_ref = self.client.dataset(str(dataset_id))
            
            try:
                for table in self.client.list_tables(dataset_ref):
                    if table.table_type == 'TABLE':  # Exclude views
                        tables.append(table.table_id)
                all_tables[dataset_id] = tables
            except Exception as e:
                print(f"Error accessing dataset {dataset_id}: {e}")
                all_tables[dataset_id] = []
        
        return all_tables
    
    def get_table_schema(self, dataset_id: str, table_id: str) -> List[Dict]:
        """Get schema information for a specific table"""
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = self.client.get_table(table_ref)
            
            schema_info = []
            for field in table.schema:
                schema_info.append({
                    'name': field.name,
                    'type': field.field_type,
                    'mode': field.mode,
                    'description': field.description or 'No description'
                })
            return schema_info
        except Exception as e:
            print(f"Error getting schema for {dataset_id}.{table_id}: {e}")
            return []
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a BigQuery query and return results as DataFrame"""
        try:
            query_job = self.client.query(query)
            return query_job.to_dataframe()
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()
    
    def get_sample_data(self, dataset_id: str, table_id: str, limit: int = 5) -> pd.DataFrame:
        """Get sample data from a table"""
        query = f"""
        SELECT * 
        FROM `{self.project_id}.{dataset_id}.{table_id}` 
        LIMIT {limit}
        """
        return self.execute_query(query)
    
