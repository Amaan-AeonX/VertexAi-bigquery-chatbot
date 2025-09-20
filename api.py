from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import json
import asyncio
import os
from dotenv import load_dotenv
from bigquery_client import BigQueryClient
from simple_ai_client import SimpleAIClient

load_dotenv()
os.environ['GOOGLE_CLOUD_PROJECT'] = 'raymond-maini-iiot'

app = FastAPI(title="Manufacturing Chatbot API")

class ChatRequest(BaseModel):
    question: str

class ChatbotService:
    def __init__(self):
        self.bq_client = BigQueryClient()
        self.ai_client = SimpleAIClient()
        self.table_schemas = self._load_schemas()
    
    def _load_schemas(self):
        all_tables = self.bq_client.get_all_tables()
        schemas = {}
        for dataset_id, tables in all_tables.items():
            schemas[dataset_id] = {}
            for table_id in tables:
                schemas[dataset_id][table_id] = self.bq_client.get_table_schema(dataset_id, table_id)
        return schemas
    
    async def process_streaming(self, question: str):
        try:
            yield f"data: {json.dumps({'type': 'status', 'message': 'Generating SQL query...'})}\n\n"
            
            sql_query = self.ai_client.generate_sql_query(question, self.table_schemas)
            
            yield f"data: {json.dumps({'type': 'status', 'message': 'Executing query...'})}\n\n"
            
            results_df = self.bq_client.execute_query(sql_query)
            
            yield f"data: {json.dumps({'type': 'status', 'message': 'Generating explanation...'})}\n\n"
            
            explanation = self.ai_client.explain_results(sql_query, results_df, question)
            
            yield f"data: {json.dumps({'type': 'explanation', 'text': explanation})}\n\n"
            yield f"data: {json.dumps({'type': 'complete'})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

chatbot = ChatbotService()

@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    return StreamingResponse(
        chatbot.process_streaming(request.question),
        media_type="text/plain",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )

@app.get("/schemas")
async def get_schemas():
    return chatbot.table_schemas