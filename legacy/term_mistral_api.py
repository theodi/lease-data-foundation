import pandas as pd
import json
import requests
import os
import sys
import time
from datetime import datetime

print("="*80)
print("Extracts: Start Date, End Date, Tenor from unstructured Term field")
print("="*80)

# Configuration
INPUT_FILE = '3_NEW_records.csv' ## change as per filepath
OUTPUT_FILE = 'Term_Parsed_Results.csv'
CHECKPOINT_FILE = 'term_parsing_checkpoint.csv'

# Mistral API configuration
MISTRAL_API_KEY = os.getenv('MISTRAL_API_KEY')

if not MISTRAL_API_KEY:
    print("\nMISTRAL_API_KEY environment variable not set")
    print("\nGet API key from: https://console.mistral.ai/")
    print("Then set it:")
    print("  export MISTRAL_API_KEY='api-key-here'")
    sys.exit(1)

print(f"API key found")

# Load input data
print(f"\nLoading {INPUT_FILE}...")
try:
    df = pd.read_csv(INPUT_FILE, dtype=str)
    print(f"Loaded {len(df):,} records")
except FileNotFoundError:
    print(f"File not found: {INPUT_FILE}")
    sys.exit(1)

if 'Unique Identifier' not in df.columns or 'Term' not in df.columns:
    print(f"Required columns missing!")
    sys.exit(1)

# Check for checkpoint
start_idx = 0
if os.path.exists(CHECKPOINT_FILE):
    print(f"\nFound checkpoint file")
    response = input("Resume from checkpoint? (y/n): ").strip().lower()
    if response == 'y':
        checkpoint_df = pd.read_csv(CHECKPOINT_FILE, dtype=str)
        start_idx = len(checkpoint_df)
        print(f"  Resuming from record {start_idx:,}")
    else:
        os.remove(CHECKPOINT_FILE)

def parse_term_with_mistral_api(term_text, api_key, retry_count=0):
    """Parse lease term using Mistral API with improved prompt"""
    
    prompt = f"""You are a specialist in parsing UK property lease terms with expertise in handling:
- Various date formats (DD/MM/YYYY, MM/DD/YYYY, DD-MM-YYYY, text months, etc.)
- Typos and inconsistent formatting
- Missing information (not all fields always present)
- Fields in any order (start date may come before or after end date)

Parse this UK lease term: "{term_text}"

Extract and return JSON with these three fields:
- start_date: The lease start/commencement date (use DD/MM/YYYY format if possible, or keep original if ambiguous)
- end_date: The lease end/expiry date (use DD/MM/YYYY format if possible, or keep original if ambiguous)
- tenor: The lease duration/term (e.g., "99 years", "125 years", "999 years less 1 day")

IMPORTANT RULES:
1. If a field is NOT present in the text, use "Not specified"
2. For dates: Accept both UK (DD/MM/YYYY) and US (MM/DD/YYYY) formats - use context to determine
3. Handle typos intelligently (e.g., "FEbruary", "June1973", "01.03.2001")
4. Extract tenor even if dates are present (e.g., "125 years from 01/01/2000")
5. If only tenor is given without end date, that's fine - use "Not specified" for end_date
6. For complex tenors like "999 years less 1 day", keep the full description

Return ONLY valid JSON (no markdown, no explanation):
{{"start_date": "...", "end_date": "...", "tenor": "..."}}"""

    try:
        response = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={
                "ization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "mistral-small-latest",
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.0,  # Lower temperature for more consistent parsing
                "max_tokens": 300
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            
            # Extract JSON from response
            start_idx = content.find('{')
            end_idx = content.rfind('}') + 1
            
            if start_idx >= 0 and end_idx > start_idx:
                json_str = content[start_idx:end_idx]
                parsed = json.loads(json_str)
                
                return {
                    'Start_Date': parsed.get('start_date', 'Not specified'),
                    'End_Date': parsed.get('end_date', 'Not specified'),
                    'Tenor': parsed.get('tenor', 'Not specified'),
                    'Status': 'Success'
                }
            else:
                return {
                    'Start_Date': 'No JSON',
                    'End_Date': 'No JSON',
                    'Tenor': 'No JSON',
                    'Status': 'No JSON in response'
                }
                
        elif response.status_code == 429:
            # Rate limit hit - exponential backoff
            if retry_count < 3:
                wait_time = (2 ** retry_count) * 2  # 2, 4, 8 seconds
                print(f"    Rate limit hit, waiting {wait_time}s before retry {retry_count + 1}/3...")
                time.sleep(wait_time)
                return parse_term_with_mistral_api(term_text, api_key, retry_count + 1)
            else:
                return {
                    'Start_Date': 'Rate Limit',
                    'End_Date': 'Rate Limit',
                    'Tenor': 'Rate Limit',
                    'Status': 'Rate limit exceeded after retries'
                }
        else:
            return {
                'Start_Date': 'API Error',
                'End_Date': 'API Error',
                'Tenor': 'API Error',
                'Status': f'API Error: {response.status_code}'
            }
            
    except json.JSONDecodeError as e:
        return {
            'Start_Date': 'JSON Error',
            'End_Date': 'JSON Error',
            'Tenor': 'JSON Error',
            'Status': f'JSON Error: {str(e)}'
        }
    except requests.exceptions.Timeout:
        return {
            'Start_Date': 'Timeout',
            'End_Date': 'Timeout',
            'Tenor': 'Timeout',
            'Status': 'Request timeout'
        }
    except Exception as e:
        return {
            'Start_Date': 'Error',
            'End_Date': 'Error',
            'Tenor': 'Error',
            'Status': f'Error: {str(e)}'
        }

# Process records
print("\n" + "="*80)
print("PROCESSING WITH MISTRAL API")
print("="*80)

results = []

# Load checkpoint if resuming
if start_idx > 0:
    checkpoint_df = pd.read_csv(CHECKPOINT_FILE, dtype=str)
    results = checkpoint_df.to_dict('records')

print(f"\nProcessing records {start_idx:,} to {len(df):,}")
print("Rate limiting: 1 request per second (to avoid 429 errors)")
print("Press Ctrl+C to save checkpoint and exit\n")

start_time = time.time()

try:
    for idx in range(start_idx, len(df)):
        row = df.iloc[idx]
        uid = row['Unique Identifier']
        term = str(row.get('Term', ''))
        
        if idx % 5 == 0:
            pct = ((idx + 1) / len(df)) * 100
            elapsed = time.time() - start_time
            rate = (idx - start_idx + 1) / elapsed if elapsed > 0 else 0
            remaining = (len(df) - idx - 1) / rate if rate > 0 else 0
            print(f"  [{idx+1:,}/{len(df):,}] {pct:.1f}% | Rate: {rate:.1f} req/s | ETA: {remaining/60:.1f} min")
        
        # Parse with API
        parsed = parse_term_with_mistral_api(term, MISTRAL_API_KEY)
        
        # Build result
        result = {
            'Unique Identifier': uid,
            'Term_Original': term,
            'Start_Date': parsed['Start_Date'],
            'End_Date': parsed['End_Date'],
            'Tenor': parsed['Tenor'],
            'Parse_Status': parsed['Status']
        }
        
        results.append(result)
        
        # Save checkpoint every 20 records
        if (idx + 1) % 20 == 0:
            checkpoint_df = pd.DataFrame(results)
            checkpoint_df.to_csv(CHECKPOINT_FILE, index=False)
        
        # Rate limiting - 1 request per second to avoid 429
        time.sleep(1.0)

except KeyboardInterrupt:
    print("\n\n Interrupted")
    checkpoint_df = pd.DataFrame(results)
    checkpoint_df.to_csv(CHECKPOINT_FILE, index=False)
    print(f"Checkpoint saved ({len(results):,} records)")
    sys.exit(0)

# Save results
print(f"\nProcessed all {len(results):,} records")

results_df = pd.DataFrame(results)
results_df.to_csv(OUTPUT_FILE, index=False)
print(f"Saved to: {OUTPUT_FILE}")

# Clean up checkpoint
if os.path.exists(CHECKPOINT_FILE):
    os.remove(CHECKPOINT_FILE)

# Summary
print("\n" + "="*80)
print("PARSING SUMMARY")
print("="*80)

print(f"\nTotal records: {len(results_df):,}")

status_counts = results_df['Parse_Status'].value_counts()
print(f"\nParse Status:")
for status, count in status_counts.items():
    pct = (count / len(results_df)) * 100
    print(f"  {status}: {count:,} ({pct:.1f}%)")

successful = results_df[results_df['Parse_Status'] == 'Success']
print(f"\nSuccessfully parsed: {len(successful):,} ({len(successful)/len(results_df)*100:.1f}%)")

# Sample results
print("\n" + "="*80)
print("SAMPLE RESULTS (first 10)")
print("="*80)
print(results_df[['Term_Original', 'Start_Date', 'End_Date', 'Tenor']].head(10).to_string(index=False, max_colwidth=45))

errors = results_df[results_df['Parse_Status'] != 'Success']
if len(errors) > 0:
    print("\n" + "="*80)
    print("SAMPLE ERRORS (first 5)")
    print("="*80)
    print(errors[['Term_Original', 'Parse_Status']].head(5).to_string(index=False, max_colwidth=60))

print("\n" + "="*80)
print("COMPLETE")
print("="*80)
print(f"\nOutput: {OUTPUT_FILE}")
print("Columns: Unique Identifier, Term_Original, Start_Date, End_Date, Tenor, Parse_Status")