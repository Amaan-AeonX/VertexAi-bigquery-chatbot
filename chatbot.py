import streamlit as st
import os
from dotenv import load_dotenv
from bigquery_client import BigQueryClient
from vertex_ai_client import VertexAIClient
import pandas as pd

# Load environment variables
load_dotenv()

class ManufacturingChatbot:
    def __init__(self):
        self.bq_client = BigQueryClient()
        self.ai_client = VertexAIClient()
        self.table_schemas = {}
        self.load_table_schemas()
    
    def load_table_schemas(self):
        """Load all table schemas for context"""
        all_tables = self.bq_client.get_all_tables()
        
        for dataset_id, tables in all_tables.items():
            self.table_schemas[dataset_id] = {}
            for table_id in tables:
                schema = self.bq_client.get_table_schema(dataset_id, table_id)
                self.table_schemas[dataset_id][table_id] = schema
    
    def process_question(self, user_question: str):
        """Process user question and return answer"""
        try:
            # Generate SQL query using Vertex AI
            sql_query = self.ai_client.generate_sql_query(user_question, self.table_schemas)
            
            # Execute query
            results_df = self.bq_client.execute_query(sql_query)
            
            # Generate explanation
            explanation = self.ai_client.explain_results(sql_query, results_df, user_question)
            
            return {
                'sql_query': sql_query,
                'results': results_df,
                'explanation': explanation,
                'success': True
            }
        
        except Exception as e:
            return {
                'error': str(e),
                'success': False
            }

def main():
    st.set_page_config(
        page_title="Manufacturing Data Chatbot",
        page_icon="🏭",
        layout="wide"
    )
    
    st.title("🏭 Manufacturing Data Chatbot")
    st.markdown("Ask questions about your manufacturing data in plain English!")
    
    # Initialize chatbot
    if 'chatbot' not in st.session_state:
        with st.spinner("Loading data schemas..."):
            st.session_state.chatbot = ManufacturingChatbot()
    
    # Display available tables
    with st.sidebar:
        st.header("📊 Available Data")
        chatbot = st.session_state.chatbot
        
        for dataset_id, tables in chatbot.table_schemas.items():
            st.subheader(f"Dataset: {dataset_id}")
            for table_name, schema in tables.items():
                with st.expander(f"📋 {table_name}"):
                    for field in schema[:5]:  # Show first 5 fields
                        st.text(f"• {field['name']} ({field['type']})")
                    if len(schema) > 5:
                        st.text(f"... and {len(schema) - 5} more fields")
    
    # Chat interface
    st.header("💬 Ask Your Question")
    
    # Example questions
    st.markdown("**Example questions:**")
    example_questions = [
        "Show me the latest production data",
        "What are the top performing machines?",
        "Show me quality metrics for this month",
        "Which products have the highest defect rates?",
        "Show me machine downtime analysis"
    ]
    
    for i, example in enumerate(example_questions):
        if st.button(f"📝 {example}", key=f"example_{i}"):
            st.session_state.user_input = example
    
    # User input
    user_question = st.text_input(
        "Your question:",
        value=st.session_state.get('user_input', ''),
        placeholder="e.g., Show me production data for the last week"
    )
    
    if st.button("🚀 Get Answer", type="primary"):
        if user_question:
            with st.spinner("Analyzing your question and fetching data..."):
                result = st.session_state.chatbot.process_question(user_question)
            
            if result['success']:
                # Show explanation
                st.success("✅ Here's what I found:")
                st.markdown(result['explanation'])
                
                # Show data
                if not result['results'].empty:
                    st.subheader("📊 Data Results")
                    st.dataframe(result['results'], use_container_width=True)
                    
                    # Download option
                    csv = result['results'].to_csv(index=False)
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv,
                        file_name="query_results.csv",
                        mime="text/csv"
                    )
                
                # Show SQL query (expandable)
                with st.expander("🔍 View SQL Query"):
                    st.code(result['sql_query'], language='sql')
            
            else:
                st.error(f"❌ Error: {result['error']}")
        else:
            st.warning("Please enter a question!")
    
    # Clear input
    if st.button("🗑️ Clear"):
        st.session_state.user_input = ""
        st.rerun()

if __name__ == "__main__":
    main()