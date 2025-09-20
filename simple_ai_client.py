from typing import Dict, List
import pandas as pd

class SimpleAIClient:
    def __init__(self):
        pass
    
    def generate_sql_query(self, user_question: str, table_schemas: Dict) -> str:
        """Generate basic SQL query based on keywords"""
        question_lower = user_question.lower()
        
        # Extract machine code from question
        import re
        machine_code_match = re.search(r'\b([A-Z]{2,}\d{2,})\b', user_question)
        machine_code = machine_code_match.group(1) if machine_code_match else None
        
        # Machine parameters query - comprehensive list
        machine_param_keywords = [
            'feed', 'rate', 'spindle', 'speed', 'rpm', 'axis', 'position', 'alarm', 'cutting', 'cycle', 
            'emergency', 'operating', 'part', 'count', 'power', 'program', 'servo', 'load', 'tool', 
            'offset', 'diagnosis', 'path', 'sequence', 'number'
        ]
        
        if any(word in question_lower for word in machine_param_keywords) and machine_code:
            if 'spindle' in question_lower:
                return f"SELECT machine_code, spindle_speed, spindel_speed, created_at FROM `raymond-maini-iiot.cnc_dataset.machine_parameters` WHERE machine_code = '{machine_code}' ORDER BY created_at DESC LIMIT 1"
            elif 'feed' in question_lower:
                return f"SELECT machine_code, feed_rate, actual_feed_rate, created_at FROM `raymond-maini-iiot.cnc_dataset.machine_parameters` WHERE machine_code = '{machine_code}' ORDER BY created_at DESC LIMIT 1"
            return f"SELECT machine_code, spindle_speed, spindel_speed, feed_rate, actual_feed_rate, created_at FROM `raymond-maini-iiot.cnc_dataset.machine_parameters` WHERE machine_code = '{machine_code}' ORDER BY created_at DESC LIMIT 1"
        
        # Machine uptime/downtime query
        if any(word in question_lower for word in ['uptime', 'downtime', 'availability']) and machine_code:
            return f"SELECT machine_code, total_uptime_hours, total_downtime_hours, first_created_at FROM `raymond-maini-iiot.cnc_dataset.machine_uptime_downtime` WHERE machine_code = '{machine_code}' ORDER BY first_created_at DESC LIMIT 1"
        
        # Machine status query
        if any(word in question_lower for word in ['status', 'machine']) and machine_code:
            return f"SELECT machine_code, machine_status, connection_status, created_at FROM `raymond-maini-iiot.cnc_dataset.machine_connections` WHERE machine_code = '{machine_code}' ORDER BY created_at DESC LIMIT 1"
        
        # Latest/recent queries
        if any(word in question_lower for word in ['latest', 'recent', 'new']):
            for dataset, tables in table_schemas.items():
                for table_name, schema in tables.items():
                    for field in schema:
                        if any(time_word in field['name'].lower() for time_word in ['time', 'date', 'created', 'updated']):
                            return f"SELECT * FROM `raymond-maini-iiot.cnc_dataset.{table_name}` ORDER BY {field['name']} DESC LIMIT 10"
        
        # Count queries
        if any(word in question_lower for word in ['count', 'total', 'number']):
            for dataset, tables in table_schemas.items():
                for table_name in tables.keys():
                    return f"SELECT COUNT(*) as total_records FROM `raymond-maini-iiot.cnc_dataset.{table_name}`"
        
        # Default: show sample data from first table
        for dataset, tables in table_schemas.items():
            for table_name in tables.keys():
                return f"SELECT * FROM `raymond-maini-iiot.cnc_dataset.{table_name}` LIMIT 5"
        
        return "SELECT 1 as result"
    
    def explain_results(self, query: str, results_df: pd.DataFrame, user_question: str) -> str:
        """Generate user-friendly explanation"""
        if results_df.empty:
            return "No data found for your query."
        
        question_lower = user_question.lower()
        
        # Machine uptime/downtime response
        if any(word in question_lower for word in ['uptime', 'downtime', 'availability']):
            if len(results_df) > 0:
                machine_code = results_df.iloc[0].get('machine_code', 'Unknown')
                uptime = results_df.iloc[0].get('total_uptime_hours', 'N/A')
                downtime = results_df.iloc[0].get('total_downtime_hours', 'N/A')
                first_created_at = results_df.iloc[0].get('first_created_at', 'Unknown')
                
                if uptime != 'N/A' and downtime != 'N/A':
                    total_time = float(uptime) + float(downtime)
                    availability = (float(uptime) / total_time * 100) if total_time > 0 else 0
                    return f"Machine {machine_code} - Uptime: {uptime} hours, Downtime: {downtime} hours, Availability: {availability:.1f}% (as of {first_created_at})"
                else:
                    return f"Machine {machine_code} - Uptime: {uptime} hours, Downtime: {downtime} hours (as of {first_created_at})"
        
        # Machine parameters response
        machine_param_keywords = [
            'feed', 'rate', 'spindle', 'speed', 'rpm', 'axis', 'position', 'alarm', 'cutting', 'cycle', 
            'emergency', 'operating', 'part', 'count', 'power', 'program', 'servo', 'load', 'tool', 
            'offset', 'diagnosis', 'path', 'sequence', 'number'
        ]
        
        if any(word in question_lower for word in machine_param_keywords):
            if len(results_df) > 0:
                machine_code = results_df.iloc[0].get('machine_code', 'Unknown')
                
                if 'spindle' in question_lower:
                    spindle_speed = results_df.iloc[0].get('spindle_speed') or results_df.iloc[0].get('spindel_speed', 'N/A')
                    created_at = results_df.iloc[0].get('created_at', 'Unknown')
                    return f"Machine {machine_code} spindle speed is {spindle_speed} RPM (as of {created_at})"
                elif 'feed' in question_lower:
                    feed_rate = results_df.iloc[0].get('actual_feed_rate') or results_df.iloc[0].get('feed_rate', 'N/A')
                    created_at = results_df.iloc[0].get('created_at', 'Unknown')
                    return f"Machine {machine_code} actual feed rate is {feed_rate} (as of {created_at})"
                else:
                    spindle_speed = results_df.iloc[0].get('spindle_speed') or results_df.iloc[0].get('spindel_speed', 'N/A')
                    feed_rate = results_df.iloc[0].get('feed_rate') or results_df.iloc[0].get('actual_feed_rate', 'N/A')
                    created_at = results_df.iloc[0].get('created_at', 'Unknown')
                    return f"Machine {machine_code} parameters: Spindle Speed: {spindle_speed} RPM, Feed Rate: {feed_rate}. Last updated: {created_at}"
        
        # Machine status specific response
        if 'status' in question_lower and 'machine' in question_lower:
            if len(results_df) > 0:
                machine_code = results_df.iloc[0].get('machine_code', 'Unknown')
                machine_status = results_df.iloc[0].get('machine_status', 'Unknown')
                connection_status = results_df.iloc[0].get('connection_status', 'Unknown')
                created_at = results_df.iloc[0].get('created_at', 'Unknown')
                
                return f"Machine {machine_code} is currently {machine_status.lower()}. Connection status: {connection_status}. Last updated: {created_at}"
        
        # Count queries
        if 'count' in question_lower or 'total' in question_lower:
            if 'total_records' in results_df.columns:
                total = results_df.iloc[0]['total_records']
                return f"Total number of records: {total}"
        
        # Latest/recent queries
        if 'latest' in question_lower or 'recent' in question_lower:
            return f"Here are the {len(results_df)} most recent records from your manufacturing data."
        
        # Default response - show actual data
        if len(results_df) > 0:
            # Get first record data
            record = results_df.iloc[0]
            response_parts = []
            
            for col, value in record.items():
                if col not in ['created_at', 'first_created_at', 'timestamp'] or len(response_parts) < 3:
                    response_parts.append(f"{col}: {value}")
            
            return "Data found: " + ", ".join(response_parts[:5])  # Limit to 5 fields
        
        return "No data found."