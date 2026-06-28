import requests

# Your Catalyst serverless function URL
url = "https://tavily-search-907381267.development.catalystserverless.com/server/tavily_search_function/execute"

# Test query (Stack Overflow search)
query = "python threading timeout in serverless"

# Method 1: POST with JSON body (recommended)
response = requests.post(
    url,
    json={"query": query},  # JSON body with query parameter
    headers={"Content-Type": "application/json"},
    timeout=30  # 30s timeout for serverless response
)

# Method 2: POST with form data (alternative)
# response = requests.post(
#     url,
#     data={"query": query},  # Form-encoded
#     timeout=30
# )

# Method 3: GET with query parameter (also works)
# response = requests.get(
#     f"{url}?query={query}",  # Query string parameter
#     timeout=30
# )

# Check response
if response.status_code == 200:
    result = response.json()
    print("Success!")
    print(f"Query: {result.get('query')}")
    print(f"Results found: {result.get('results_found', 0)}")
    print(f"Total time: {result.get('metadata', {}).get('timing', {}).get('total_time', 'N/A')}s")
    
    # Print first successful result
    if result.get('data'):
        first_result = result['data'][0]
        print(f"\nFirst result from: {first_result.get('url')}")
        print(f"Title: {first_result.get('title')}")
        print(f"Question: {first_result.get('question')[:100]}...")
else:
    print(f"Error {response.status_code}: {response.text}")
