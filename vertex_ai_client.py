from google.cloud import aiplatform
from typing import Dict
import vertexai
from vertexai.generative_models import GenerativeModel
import pandas as pd

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
        Generate a BigQuery SQL query based on the user's question and available table schemas.
        
        {schema_context}
        
        Question: {user_question}
        
        RULES:
        1. Use full table references: `raymond-maini-iiot.dataset.table`
        2. Extract machine codes from question (format: 2+ letters + 2+ digits like CTC074, VMC153)
        3. For time queries use TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL X HOUR/DAY)
        4. Always include ORDER BY with timestamp columns DESC
        5. Use appropriate LIMIT:
           - If user says "all" or "show all": use LIMIT 100 or no LIMIT
           - Otherwise: use LIMIT 10-20
        6. Match user intent to correct tables:
           - "machine types" or "types of machines" → dev_public.machine_type
           - "machine details" or "machine info" → dev_public.machine_details
           - "machine status" or "status" → cnc_dataset.machine_parameters
           - "machine parameters" or "feed rate" or "spindle speed" → cnc_dataset.machine_parameters
           - "uptime" or "downtime" → cnc_dataset.machine_uptime_downtime
        7. For useful columns:
           - machine_type: name, description
           - machine_details: machine_code, machine_name, machine_status, line_name, location_name
        8. NEVER include id, uuid, or similar identifier columns in SELECT
        9. ALWAYS add WHERE conditions to exclude NULL values for main columns
        10. For counting running machines: use COUNT(DISTINCT machine_code) to count unique machines only
        11. For "current" or "currently running/idle" machines: use timestamp filter within last 2 minutes:
            - Running: WHERE machine_status = 'Running' AND timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE) AND CURRENT_TIMESTAMP()
            - Idle: WHERE machine_status = 'Idle' AND timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE) AND CURRENT_TIMESTAMP()
        
        Generate ONLY the SQL query:
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
        
        # Prepare results summary with actual data
        if len(results_df) == 0:
            results_summary = "Query returned 0 rows - no data found"
        else:
            results_summary = f"Query returned {len(results_df)} rows with columns: {', '.join(results_df.columns.tolist())}"
            # Show all data if user asked for "all" or if small dataset
            if 'all' in user_question.lower() or len(results_df) <= 10:
                results_summary += f"\n\nAll data:\n{results_df.to_string()}"
            else:
                results_summary += f"\n\nFirst 5 rows:\n{results_df.head(5).to_string()}"
        
        prompt = f"""
        Give a one-line explanation followed by the data.
        
        User asked: {user_question}
        Results: {results_summary}
        
        Format:
        - Start with ONE short sentence explaining what was found
        - Then present the data clearly
        - If no data: brief explanation why
        - Keep explanation to maximum 10 words
        """
        
        vertexai.init(project=self.project_id, location=self.location)
        model = GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        
        return response.text.strip()