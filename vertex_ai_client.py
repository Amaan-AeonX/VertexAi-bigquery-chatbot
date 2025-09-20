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
        You are a SQL expert. Based on the user's question and the available BigQuery table schemas, generate a SQL query.
        
        {schema_context}
        
        User Question: {user_question}
        
        Rules:
        1. Only use tables that exist in the schemas above
        2. Use proper BigQuery syntax
        3. Include the full table reference: `project.dataset.table`
        4. Keep queries simple and efficient
        5. If the question is unclear, generate a query that shows relevant sample data
        
        Generate only the SQL query without any explanation:
        """
        
        # Using Vertex AI's Gemini model
        import vertexai
        from vertexai.generative_models import GenerativeModel
        
        vertexai.init(project=self.project_id, location=self.location)
        model = GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        
        return response.text.strip()
    
    def explain_results(self, query: str, results_df, user_question: str) -> str:
        """Generate human-friendly explanation of query results"""
        
        # Prepare results summary
        results_summary = f"Query returned {len(results_df)} rows"
        if len(results_df) > 0:
            results_summary += f" with columns: {', '.join(results_df.columns.tolist())}"
            if len(results_df) <= 10:
                results_summary += f"\n\nSample data:\n{results_df.to_string()}"
            else:
                results_summary += f"\n\nFirst 5 rows:\n{results_df.head().to_string()}"
        
        prompt = f"""
        You are a data analyst explaining query results to a business user in simple terms.
        
        User's original question: {user_question}
        
        SQL Query executed: {query}
        
        Results summary: {results_summary}
        
        Provide a clear, easy-to-understand explanation of what the data shows in response to the user's question.
        Use simple language and avoid technical jargon. Focus on business insights.
        """
        
        import vertexai
        from vertexai.generative_models import GenerativeModel
        
        vertexai.init(project=self.project_id, location=self.location)
        model = GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        
        return response.text.strip()