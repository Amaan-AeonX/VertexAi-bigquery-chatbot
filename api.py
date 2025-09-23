from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import json
import os
from dotenv import load_dotenv
from bigquery_client import BigQueryClient
from vertex_ai_client import VertexAIClient

load_dotenv()
os.environ['GOOGLE_CLOUD_PROJECT'] = 'raymond-maini-iiot'

app = FastAPI(title="Manufacturing Chatbot API")

class ChatRequest(BaseModel):
    question: str

class ChatbotService:
    def __init__(self):
        self.bq_client = BigQueryClient()
        self.ai_client = VertexAIClient()
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
            # Handle pure greetings only (not mixed with questions)
            question_lower = question.lower().strip()
            pure_greetings = ['hello', 'hi', 'hii', 'hey', 'good morning', 'good afternoon', 'good evening']
            if question_lower in pure_greetings:
                explanation = "Hello! I'm your Manufex AI Data Assistant. I can help you with machine status, parameters, uptime, and other manufacturing data queries. What would you like to know?"
                yield json.dumps({'answer': explanation}) + "\n"
                return
            
            # All queries processed dynamically
            sql_query = self.ai_client.generate_sql_query(question, self.table_schemas)
            print(f"Generated SQL: {sql_query}")
            results_df = self.bq_client.execute_query(sql_query)
            explanation = self.ai_client.explain_results(sql_query, results_df, question)

            yield json.dumps({'answer': explanation}) + "\n"

        except Exception as e:
            yield json.dumps({'answer': str(e)}) + "\n"

chatbot = ChatbotService()

@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    return StreamingResponse(
        chatbot.process_streaming(request.question),
        media_type="text/plain",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )