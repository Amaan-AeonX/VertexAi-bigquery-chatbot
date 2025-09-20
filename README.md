# Manufacturing Data Chatbot 🏭

A smart chatbot powered by FastAPI that allows you to query your BigQuery manufacturing datasets using natural language.

## Features

- 🗣️ **Natural Language Queries**: Ask questions in plain English
- 🔍 **Smart SQL Generation**: Automatically converts questions to SQL
- 📊 **Real-time Streaming**: Get responses as they're generated
- 🛡️ **Read-Only Access**: No data modification, only queries
- 🏭 **Manufacturing Focus**: Optimized for CNC machine data

## Quick Start

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Authenticate with Google Cloud**:
   ```bash
   gcloud auth application-default login
   ```

3. **Run the API**:
   ```bash
   uvicorn api:app --reload
   ```

4. **Test with Example Client**:
   ```bash
   python client_example.py
   ```

## API Endpoints

- `POST /chat/stream` - Streaming chat endpoint
- `GET /schemas` - Get available table schemas

## Supported Tables

- **machine_parameters** - Spindle speed, feed rate, axis positions, etc.
- **machine_connections** - Machine status and connectivity
- **machine_uptime_downtime** - Availability metrics

## Example Questions

- "Give actual feed rate of machine with code VMC153"
- "What is the spindle speed of VMC067?"
- "Show machine status for CTC123"
- "What is the uptime of VMC153?"

## Configuration

Update `.env` file with your specific details:
- `GOOGLE_CLOUD_PROJECT`: Your GCP project ID
- `BIGQUERY_DATASET`: Dataset name (cnc_dataset)
- `VERTEX_AI_LOCATION`: Vertex AI region