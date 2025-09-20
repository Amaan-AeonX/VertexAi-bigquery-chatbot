import os
from google.cloud import aiplatform
from typing import Dict, List
import json

GOOGLE_CLOUD_PROJECT="raymond-maini-iiot"
VERTEX_AI_LOCATION="asia-south1"

class VertexAIClient:
    def __init__(self):
        self.project_id = GOOGLE_CLOUD_PROJECT
        self.location = VERTEX_AI_LOCATION
        aiplatform.init(project=self.project_id, location=self.location)
    
    def generate_sql_query(self, user_question: str, table_schemas: Dict) -> str:
        """Generate SQL query based on user question and table schemas"""
        
        # Create context about available tables and their schemas
        schema_context = "Available tables and their schemas:\n\n"
        for dataset, tables in table_schemas.items():
            schema_context += f"Dataset: {dataset}\n"
            for table_name, schema in tables.items():
                schema_context += f"  Table: {table_name}\n"
                for field in schema:
                    schema_context += f"    - {field['name']} ({field['type']}): {field['description']}\n"
                schema_context += "\n"
        
        # Extract machine code from question
        import re
        machine_match = re.search(r'\b([A-Z]{2,}\d{2,})\b', user_question)
        machine_code = machine_match.group(1) if machine_match else 'CTC074'
        
        # For time-based queries, start with broader search
        if 'last 24 hours' in user_question.lower() or 'running status' in user_question.lower():
            return f"SELECT machine_code, machine_status, created_at FROM `raymond-maini-iiot.cnc_dataset.machine_connections` WHERE machine_code = '{machine_code}' ORDER BY created_at DESC LIMIT 50"
        
        prompt = f"""
        Generate a BASIC BigQuery SELECT query.
        
        {schema_context}
        
        Question: {user_question}
        
        RULES:
        1. Use tables from: `raymond-maini-iiot.cnc_dataset.*` or `raymond-maini-iiot.dev_public.*`
        2. For machine codes, use: WHERE machine_code = 'CODE'
        3. Always ORDER BY created_at DESC LIMIT 20
        4. Only use: SELECT, FROM, WHERE, ORDER BY, LIMIT
        
        Generate ONLY basic SELECT query:
        """
        
        # Using Vertex AI's Gemini model
        import vertexai
        from vertexai.generative_models import GenerativeModel
        
        vertexai.init(project=self.project_id, location=self.location)
        model = GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        
        sql = response.text.strip().replace('```sql', '').replace('```', '').strip()
        return sql
    
    def explain_results(self, query: str, results_df, user_question: str) -> str:
        """Generate human-friendly explanation of query results"""
        
        if len(results_df) == 0:
            return "No data found for your query. The machine code might not exist in our database, there might be no data in the specified time period, or the machine might not have had any status changes recently."
        
        # Prepare results summary with actual data
        results_summary = f"Query returned {len(results_df)} rows with columns: {', '.join(results_df.columns.tolist())}"
        if len(results_df) <= 5:
            results_summary += f"\n\nActual data:\n{results_df.to_string()}"
        else:
            results_summary += f"\n\nFirst 3 rows:\n{results_df.head(3).to_string()}"
        
        # Special handling for running time questions
        if 'running status' in user_question.lower() and 'how long' in user_question.lower():
            running_records = results_df[results_df['machine_status'].str.contains('Running', case=False, na=False)] if 'machine_status' in results_df.columns else pd.DataFrame()
            if not running_records.empty:
                total_running_records = len(running_records)
                latest_status = results_df.iloc[0]['machine_status'] if len(results_df) > 0 else 'Unknown'
                return f"Found {total_running_records} records where machine was in Running status out of {len(results_df)} total status records. Current status: {latest_status}. Note: Exact duration calculation requires analyzing status change intervals."
        
        prompt = f"""
        Explain manufacturing data results in simple business terms.
        
        User asked: {user_question}
        
        Results: {results_summary}
        
        Provide a clear explanation focusing on:
        1. What specific data was found
        2. Key values (machine status, parameters, etc.)
        3. Timestamps when available
        4. Business meaning of the data
        
        Keep it concise and practical.
        """
        
        import vertexai
        from vertexai.generative_models import GenerativeModel
        import pandas as pd
        
        vertexai.init(project=self.project_id, location=self.location)
        model = GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        
        return response.text.strip()