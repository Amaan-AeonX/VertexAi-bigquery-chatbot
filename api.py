from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import json
import asyncio
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
            if 'running status' in question.lower() and 'how long' in question.lower():
                import re
                machine_match = re.search(r'\b([A-Z]{2,}\d{2,})\b', question)
                if machine_match:
                    machine_code = machine_match.group(1)
                    running_time = self.bq_client.calculate_running_time(machine_code, 24)
                    
                    if running_time['running_hours'] > 0:
                        explanation = (
                            f"Machine {machine_code} was running for "
                            f"{running_time['running_hours']} hours "
                            f"({running_time['running_minutes']} minutes) in the last 24 hours."
                        )
                    else:
                        explanation = (
                            f"Machine {machine_code} was not in running status in the last 24 hours, "
                            f"or no status change data is available."
                        )

                    yield json.dumps({'answer': explanation}) + "\n"
                    return

            # Regular query
            sql_query = self.ai_client.generate_sql_query(question, self.table_schemas)
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