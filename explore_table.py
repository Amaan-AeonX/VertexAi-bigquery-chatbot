from bigquery_client import BigQueryClient

def explore_machine_parameters():
    client = BigQueryClient()
    
    # Get table schema
    print("=== Machine Parameters Table Schema ===")
    schema = client.get_table_schema("cnc_dataset", "machine_uptime_downtime")
    for field in schema:
        print(f"- {field['name']} ({field['type']}): {field['description']}")
    
    # Get sample data
    print("\n=== Sample Data ===")
    sample_df = client.get_sample_data("cnc_dataset", "machine_uptime_downtime", 3)
    print(sample_df.to_string())
    
    # Get column names
    print(f"\n=== Available Columns ===")
    if not sample_df.empty:
        print(sample_df.columns.tolist())

if __name__ == "__main__":
    explore_machine_parameters()