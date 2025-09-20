import requests
import json

def stream_chat(question: str, api_url: str = "http://localhost:8000"):
    """Example client for streaming chat API"""
    
    response = requests.post(
        f"{api_url}/chat/stream",
        json={"question": question},
        stream=True
    )
    
    print(f"Question: {question}\n")
    
    for line in response.iter_lines():
        if line:
            line = line.decode('utf-8')
            if line.startswith('data: '):
                data = json.loads(line[6:])
                
                if data['type'] == 'status':
                    print(f"Status: {data['message']}")
                elif data['type'] == 'explanation':
                    print(f"Answer: {data['text']}")
                elif data['type'] == 'error':
                    print(f"Error: {data['message']}")
                elif data['type'] == 'complete':
                    print("Complete!")

if __name__ == "__main__":
    stream_chat("Give actual feed rate of machine with code VMC153")