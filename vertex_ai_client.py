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
        
        prompt = f"""
        You are a SQL expert for manufacturing data. Generate a BigQuery SQL query based on the user's question.
        
        {schema_context}
        
        User Question: {user_question}
        
        IMPORTANT RULES:
        1. Use exact table reference: `raymond-maini-iiot.cnc_dataset.table_name`
        2. For machine codes (like VMC153, CTC099), use WHERE machine_code = 'CODE'
        3. Always ORDER BY created_at DESC or timestamp DESC for latest data
        4. Use LIMIT 10 for safety unless asking for counts
        5. Common patterns:
           - Machine status: SELECT * FROM `raymond-maini-iiot.cnc_dataset.machine_connections` WHERE machine_code = 'XXX'
           - Machine parameters: SELECT * FROM `raymond-maini-iiot.cnc_dataset.machine_parameters` WHERE machine_code = 'XXX'
           - Uptime/downtime: SELECT * FROM `raymond-maini-iiot.cnc_dataset.machine_uptime_downtime` WHERE machine_code = 'XXX'
        
        Generate ONLY the SQL query:
        """
        
        # Using Vertex AI's Gemini model
        import vertexai
        from vertexai.generative_models import GenerativeModel
        
        vertexai.init(project=self.project_id, location=self.location)
        model = GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        
        return response.text.strip().replace('```sql', '').replace('```', '').strip()
    
    def explain_results(self, query: str, results_df, user_question: str) -> str:
        """Generate human-friendly explanation of query results"""
        
        if len(results_df) == 0:
            return "No data found for your query. The machine code might not exist in our database or there might be no recent data available."
        
        # Prepare results summary with actual data
        results_summary = f"Query returned {len(results_df)} rows with columns: {', '.join(results_df.columns.tolist())}"
        if len(results_df) <= 5:
            results_summary += f"\n\nActual data:\n{results_df.to_string()}"
        else:
            results_summary += f"\n\nFirst 3 rows:\n{results_df.head(3).to_string()}"
        
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
        
        vertexai.init(project=self.project_id, location=self.location)
        model = GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        
        return response.text.strip()