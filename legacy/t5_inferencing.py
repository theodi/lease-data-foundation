import pandas as pd
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration
import sys
from tqdm import tqdm
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import numpy as np
import os
import json
from datetime import datetime

# Configuration
MODEL_PATH = './'
INPUT_CSV = '4_UNCHANGED_records.csv'
TERM_COLUMN = 'Term'  # Change this to  term column name
MAX_LENGTH = 64
CHECKPOINT_INTERVAL = 250  # Save every 250 records

# Generate output filename
input_dir = os.path.dirname(INPUT_CSV)
input_filename = os.path.basename(INPUT_CSV)
input_name, input_ext = os.path.splitext(input_filename)
OUTPUT_CSV = os.path.join(input_dir, f"{input_name}_parsed{input_ext}")
CHECKPOINT_FILE = os.path.join(input_dir, f"{input_name}_checkpoint.json")
TEMP_OUTPUT_CSV = os.path.join(input_dir, f"{input_name}_parsed_temp{input_ext}")

print(f"Input file: {INPUT_CSV}")
print(f"Output file: {OUTPUT_CSV}")
print(f"Checkpoint file: {CHECKPOINT_FILE}")
print(f"Temporary output file: {TEMP_OUTPUT_CSV}")

# Load model and tokenizer
print("\nLoading model...")
try:
    tokenizer = T5Tokenizer.from_pretrained(MODEL_PATH, legacy=False)
    model = T5ForConditionalGeneration.from_pretrained(MODEL_PATH)
    model.eval()
    print("Model loaded successfully!")
except Exception as e:
    print(f"Error loading model: {e}")
    sys.exit(1)

# Load input data
print("\nLoading input data...")
try:
    df = pd.read_csv(INPUT_CSV)
    print(f"Loaded {len(df)} records")
    print(f"Columns: {df.columns.tolist()}")
    
    if TERM_COLUMN not in df.columns:
        print(f"Error: Column '{TERM_COLUMN}' not found in CSV")
        print(f"Available columns: {df.columns.tolist()}")
        sys.exit(1)
        
except Exception as e:
    print(f"Error loading CSV: {e}")
    sys.exit(1)

# Check for existing checkpoint
start_index = 0
processed_results = []

if os.path.exists(CHECKPOINT_FILE):
    print("\nFound existing checkpoint file...")
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint_data = json.load(f)
        
        start_index = checkpoint_data.get('last_processed_index', 0) + 1
        checkpoint_timestamp = checkpoint_data.get('timestamp', 'unknown')
        
        print(f"Checkpoint created at: {checkpoint_timestamp}")
        print(f"Resuming from record {start_index} of {len(df)}")
        
        # Load existing temporary results
        if os.path.exists(TEMP_OUTPUT_CSV):
            temp_df = pd.read_csv(TEMP_OUTPUT_CSV)
            print(f"Loaded {len(temp_df)} previously processed records")
            processed_results = temp_df.to_dict('records')
        else:
            print("Warning: Checkpoint exists but temporary output file not found. Starting from scratch.")
            start_index = 0
            
    except Exception as e:
        print(f"Error loading checkpoint: {e}")
        print("Starting from the beginning...")
        start_index = 0
else:
    print("\nNo checkpoint found. Starting from the beginning...")

def save_checkpoint(index, results_df):
    """Save checkpoint information and temporary results"""
    try:
        # Save checkpoint metadata
        checkpoint_data = {
            'last_processed_index': index,
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'records_processed': index + 1
        }
        
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        
        # Save temporary results
        results_df.to_csv(TEMP_OUTPUT_CSV, index=False)
        
        print(f"\n Checkpoint saved at record {index + 1}/{len(df)}")
        
    except Exception as e:
        print(f"\n Error saving checkpoint: {e}")

def parse_term(term_text):
    """Parse a single term using the model"""
    try:
        # Prepare input
        input_text = f"parse lease: {term_text}"
        input_ids = tokenizer(
            input_text,
            max_length=MAX_LENGTH,
            padding='max_length',
            truncation=True,
            return_tensors='pt'
        ).input_ids
        
        # Generate output
        with torch.no_grad():
            output_ids = model.generate(
                input_ids,
                max_length=MAX_LENGTH,
                num_beams=4,
                early_stopping=True
            )
        
        # Decode output
        parsed_output = tokenizer.decode(output_ids[0], skip_special_tokens=True)
        return parsed_output
        
    except Exception as e:
        return f"ERROR: {str(e)}"

# Process records
print("\nProcessing records...")
print(f"Starting from index {start_index}")

for idx in tqdm(range(start_index, len(df)), desc="Parsing terms", initial=start_index, total=len(df)):
    try:
        term_text = str(df.iloc[idx][TERM_COLUMN])
        parsed_result = parse_term(term_text)
        
        # Create result record
        result = df.iloc[idx].to_dict()
        result['parsed_output'] = parsed_result
        processed_results.append(result)
        
        # Save checkpoint every CHECKPOINT_INTERVAL records
        if (idx + 1) % CHECKPOINT_INTERVAL == 0:
            results_df = pd.DataFrame(processed_results)
            save_checkpoint(idx, results_df)
            
    except Exception as e:
        print(f"\nError processing record {idx}: {e}")
        # Still add the record with error info
        result = df.iloc[idx].to_dict()
        result['parsed_output'] = f"PROCESSING_ERROR: {str(e)}"
        processed_results.append(result)

# Save final results
print("\n\nSaving final results...")
try:
    final_df = pd.DataFrame(processed_results)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f" Final results saved to: {OUTPUT_CSV}")
    print(f"Total records processed: {len(final_df)}")
    
    # Clean up temporary files
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print(f" Checkpoint file removed")
    
    if os.path.exists(TEMP_OUTPUT_CSV):
        os.remove(TEMP_OUTPUT_CSV)
        print(f" Temporary output file removed")
        
except Exception as e:
    print(f" Error saving final results: {e}")
    sys.exit(1)

# Generate analysis and visualizations
print("\n" + "="*80)
print("GENERATING ANALYSIS AND VISUALIZATIONS")
print("="*80)

# Create output directory for plots
plots_dir = os.path.join(input_dir, f"{input_name}_analysis")
os.makedirs(plots_dir, exist_ok=True)
print(f"\nPlots will be saved to: {plots_dir}")

# 1. Basic Statistics
print("\n" + "-"*80)
print("BASIC STATISTICS")
print("-"*80)

total_records = len(final_df)
successful_parses = len(final_df[~final_df['parsed_output'].str.contains('ERROR|PROCESSING_ERROR', na=False)])
error_records = total_records - successful_parses
success_rate = (successful_parses / total_records) * 100

print(f"Total Records: {total_records:,}")
print(f"Successfully Parsed: {successful_parses:,}")
print(f"Errors: {error_records:,}")
print(f"Success Rate: {success_rate:.2f}%")

# 2. Parse output length distribution
print("\n" + "-"*80)
print("PARSED OUTPUT LENGTH ANALYSIS")
print("-"*80)

final_df['parsed_length'] = final_df['parsed_output'].str.len()
final_df['input_length'] = final_df[TERM_COLUMN].astype(str).str.len()

print(f"Average input length: {final_df['input_length'].mean():.1f} characters")
print(f"Average parsed output length: {final_df['parsed_length'].mean():.1f} characters")
print(f"Median parsed output length: {final_df['parsed_length'].median():.1f} characters")
print(f"Max parsed output length: {final_df['parsed_length'].max()} characters")
print(f"Min parsed output length: {final_df['parsed_length'].min()} characters")

# Plot: Length distribution
fig, axes = plt.subplots(1, 2, figsize=(15, 5))

axes[0].hist(final_df['input_length'], bins=50, edgecolor='black', alpha=0.7, color='steelblue')
axes[0].set_xlabel('Input Length (characters)')
axes[0].set_ylabel('Frequency')
axes[0].set_title('Distribution of Input Term Lengths')
axes[0].grid(True, alpha=0.3)

axes[1].hist(final_df['parsed_length'], bins=50, edgecolor='black', alpha=0.7, color='coral')
axes[1].set_xlabel('Parsed Output Length (characters)')
axes[1].set_ylabel('Frequency')
axes[1].set_title('Distribution of Parsed Output Lengths')
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(os.path.join(plots_dir, '01_length_distribution.png'), dpi=300, bbox_inches='tight')
print(f" Saved: 01_length_distribution.png")
plt.close()

# 3. Success vs Error Analysis
fig, ax = plt.subplots(figsize=(10, 6))
categories = ['Successful', 'Errors']
values = [successful_parses, error_records]
colors = ['#2ecc71', '#e74c3c']

bars = ax.bar(categories, values, color=colors, edgecolor='black', linewidth=1.5)
ax.set_ylabel('Number of Records')
ax.set_title('Parsing Success vs Errors', fontsize=14, fontweight='bold')
ax.grid(True, axis='y', alpha=0.3)

# Add value labels on bars
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{int(height):,}\n({height/total_records*100:.1f}%)',
            ha='center', va='bottom', fontsize=12, fontweight='bold')

plt.tight_layout()
plt.savefig(os.path.join(plots_dir, '02_success_vs_errors.png'), dpi=300, bbox_inches='tight')
print(f" Saved: 02_success_vs_errors.png")
plt.close()

# 4. Scatter plot: Input vs Output length
successful_df = final_df[~final_df['parsed_output'].str.contains('ERROR|PROCESSING_ERROR', na=False)]

if len(successful_df) > 0:
    fig, ax = plt.subplots(figsize=(12, 8))
    scatter = ax.scatter(successful_df['input_length'], 
                        successful_df['parsed_length'],
                        alpha=0.5, 
                        s=20,
                        c=successful_df['parsed_length'],
                        cmap='viridis')
    
    ax.set_xlabel('Input Length (characters)', fontsize=12)
    ax.set_ylabel('Parsed Output Length (characters)', fontsize=12)
    ax.set_title('Input Length vs Parsed Output Length', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    
    # Add colorbar
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('Output Length', fontsize=10)
    
    # Add diagonal reference line
    max_val = max(successful_df['input_length'].max(), successful_df['parsed_length'].max())
    ax.plot([0, max_val], [0, max_val], 'r--', alpha=0.3, linewidth=2, label='y=x reference')
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, '03_input_vs_output_length.png'), dpi=300, bbox_inches='tight')
    print(f" Saved: 03_input_vs_output_length.png")
    plt.close()

# 5. Token/Word analysis
print("\n" + "-"*80)
print("TOKEN/WORD ANALYSIS")
print("-"*80)

# Analyze parsed outputs
all_parsed_text = ' '.join(successful_df['parsed_output'].astype(str))
parsed_words = all_parsed_text.split()
word_freq = Counter(parsed_words)

print(f"Total words in parsed outputs: {len(parsed_words):,}")
print(f"Unique words: {len(word_freq):,}")
print(f"\nTop 20 most common words in parsed outputs:")

top_words = word_freq.most_common(20)
for word, count in top_words:
    print(f"  {word}: {count:,}")

# Plot: Top words
fig, ax = plt.subplots(figsize=(14, 8))
words, counts = zip(*top_words)
bars = ax.barh(range(len(words)), counts, color='teal', edgecolor='black')
ax.set_yticks(range(len(words)))
ax.set_yticklabels(words)
ax.set_xlabel('Frequency', fontsize=12)
ax.set_title('Top 20 Most Common Words in Parsed Outputs', fontsize=14, fontweight='bold')
ax.invert_yaxis()
ax.grid(True, axis='x', alpha=0.3)

# Add value labels
for i, (bar, count) in enumerate(zip(bars, counts)):
    ax.text(bar.get_width(), bar.get_y() + bar.get_height()/2,
            f' {count:,}', va='center', fontsize=10)

plt.tight_layout()
plt.savefig(os.path.join(plots_dir, '04_top_words.png'), dpi=300, bbox_inches='tight')
print(f" Saved: 04_top_words.png")
plt.close()

# 6. Error analysis (if any errors exist)
if error_records > 0:
    print("\n" + "-"*80)
    print("ERROR ANALYSIS")
    print("-"*80)
    
    error_df = final_df[final_df['parsed_output'].str.contains('ERROR|PROCESSING_ERROR', na=False)]
    
    # Categorize errors
    error_types = []
    for err in error_df['parsed_output']:
        if 'PROCESSING_ERROR' in err:
            error_types.append('Processing Error')
        elif 'ERROR' in err:
            error_types.append('Model Error')
        else:
            error_types.append('Other')
    
    error_type_counts = Counter(error_types)
    
    print("Error type breakdown:")
    for error_type, count in error_type_counts.items():
        print(f"  {error_type}: {count}")
    
    # Sample errors
    print("\nSample error records (first 5):")
    for idx, row in error_df.head().iterrows():
        print(f"\n  Record {idx}:")
        print(f"    Input: {str(row[TERM_COLUMN])[:100]}...")
        print(f"    Error: {row['parsed_output']}")

# 7. Comprehensive summary plot
fig = plt.figure(figsize=(16, 10))
gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)

# Success rate pie chart
ax1 = fig.add_subplot(gs[0, 0])
ax1.pie([successful_parses, error_records], 
        labels=['Success', 'Errors'],
        autopct='%1.1f%%',
        colors=['#2ecc71', '#e74c3c'],
        startangle=90,
        explode=(0.05, 0))
ax1.set_title('Success Rate', fontweight='bold')

# Length distribution box plot
ax2 = fig.add_subplot(gs[0, 1:])
data_to_plot = [final_df['input_length'].dropna(), 
                successful_df['parsed_length'].dropna()]
bp = ax2.boxplot(data_to_plot, labels=['Input Length', 'Output Length'],
                  patch_artist=True, showmeans=True)
for patch, color in zip(bp['boxes'], ['steelblue', 'coral']):
    patch.set_facecolor(color)
ax2.set_ylabel('Length (characters)')
ax2.set_title('Length Distribution Comparison', fontweight='bold')
ax2.grid(True, axis='y', alpha=0.3)

# Processing statistics
ax3 = fig.add_subplot(gs[1, :])
ax3.axis('off')
stats_text = f"""
PROCESSING SUMMARY
{'='*60}

Total Records Processed: {total_records:,}
Successfully Parsed: {successful_parses:,} ({success_rate:.2f}%)
Errors Encountered: {error_records:,} ({(error_records/total_records)*100:.2f}%)

INPUT STATISTICS
Mean Length: {final_df['input_length'].mean():.1f} characters
Median Length: {final_df['input_length'].median():.1f} characters
Std Dev: {final_df['input_length'].std():.1f} characters

OUTPUT STATISTICS
Mean Length: {final_df['parsed_length'].mean():.1f} characters
Median Length: {final_df['parsed_length'].median():.1f} characters
Std Dev: {final_df['parsed_length'].std():.1f} characters

VOCABULARY
Total Words: {len(parsed_words):,}
Unique Words: {len(word_freq):,}
Vocabulary Richness: {len(word_freq)/len(parsed_words)*100:.2f}%
"""
ax3.text(0.1, 0.5, stats_text, transform=ax3.transAxes,
         fontsize=11, verticalalignment='center', fontfamily='monospace',
         bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

# Top 10 words
ax4 = fig.add_subplot(gs[2, :])
top_10_words = word_freq.most_common(10)
words_10, counts_10 = zip(*top_10_words)
bars = ax4.bar(range(len(words_10)), counts_10, color='teal', edgecolor='black')
ax4.set_xticks(range(len(words_10)))
ax4.set_xticklabels(words_10, rotation=45, ha='right')
ax4.set_ylabel('Frequency')
ax4.set_title('Top 10 Most Common Words', fontweight='bold')
ax4.grid(True, axis='y', alpha=0.3)

for bar in bars:
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height,
            f'{int(height):,}', ha='center', va='bottom', fontsize=9)

plt.suptitle('Lease Term Parsing - Comprehensive Analysis', 
             fontsize=16, fontweight='bold', y=0.995)
plt.savefig(os.path.join(plots_dir, '05_comprehensive_summary.png'), dpi=300, bbox_inches='tight')
print(f" Saved: 05_comprehensive_summary.png")
plt.close()

# 8. Save detailed statistics to text file
stats_file = os.path.join(plots_dir, 'analysis_statistics.txt')
with open(stats_file, 'w') as f:
    f.write("="*80 + "\n")
    f.write("LEASE TERM PARSING - DETAILED ANALYSIS REPORT\n")
    f.write("="*80 + "\n\n")
    
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Input File: {INPUT_CSV}\n")
    f.write(f"Output File: {OUTPUT_CSV}\n\n")
    
    f.write("-"*80 + "\n")
    f.write("PROCESSING SUMMARY\n")
    f.write("-"*80 + "\n")
    f.write(f"Total Records: {total_records:,}\n")
    f.write(f"Successfully Parsed: {successful_parses:,} ({success_rate:.2f}%)\n")
    f.write(f"Errors: {error_records:,} ({(error_records/total_records)*100:.2f}%)\n\n")
    
    f.write("-"*80 + "\n")
    f.write("INPUT LENGTH STATISTICS\n")
    f.write("-"*80 + "\n")
    f.write(f"Mean: {final_df['input_length'].mean():.2f} characters\n")
    f.write(f"Median: {final_df['input_length'].median():.2f} characters\n")
    f.write(f"Std Dev: {final_df['input_length'].std():.2f} characters\n")
    f.write(f"Min: {final_df['input_length'].min()} characters\n")
    f.write(f"Max: {final_df['input_length'].max()} characters\n")
    f.write(f"25th Percentile: {final_df['input_length'].quantile(0.25):.2f} characters\n")
    f.write(f"75th Percentile: {final_df['input_length'].quantile(0.75):.2f} characters\n\n")
    
    f.write("-"*80 + "\n")
    f.write("OUTPUT LENGTH STATISTICS\n")
    f.write("-"*80 + "\n")
    f.write(f"Mean: {final_df['parsed_length'].mean():.2f} characters\n")
    f.write(f"Median: {final_df['parsed_length'].median():.2f} characters\n")
    f.write(f"Std Dev: {final_df['parsed_length'].std():.2f} characters\n")
    f.write(f"Min: {final_df['parsed_length'].min()} characters\n")
    f.write(f"Max: {final_df['parsed_length'].max()} characters\n")
    f.write(f"25th Percentile: {final_df['parsed_length'].quantile(0.25):.2f} characters\n")
    f.write(f"75th Percentile: {final_df['parsed_length'].quantile(0.75):.2f} characters\n\n")
    
    f.write("-"*80 + "\n")
    f.write("VOCABULARY ANALYSIS\n")
    f.write("-"*80 + "\n")
    f.write(f"Total Words: {len(parsed_words):,}\n")
    f.write(f"Unique Words: {len(word_freq):,}\n")
    f.write(f"Vocabulary Richness: {len(word_freq)/len(parsed_words)*100:.2f}%\n\n")
    
    f.write("-"*80 + "\n")
    f.write("TOP 50 MOST COMMON WORDS\n")
    f.write("-"*80 + "\n")
    for i, (word, count) in enumerate(word_freq.most_common(50), 1):
        f.write(f"{i:2d}. {word:20s} : {count:6,d} ({count/len(parsed_words)*100:.2f}%)\n")
    
    if error_records > 0:
        f.write("\n" + "-"*80 + "\n")
        f.write("ERROR ANALYSIS\n")
        f.write("-"*80 + "\n")
        for error_type, count in error_type_counts.items():
            f.write(f"{error_type}: {count}\n")

print(f" Saved: analysis_statistics.txt")

print("\n" + "="*80)
print("ANALYSIS COMPLETE!")
print("="*80)
print(f"\nAll results saved to: {plots_dir}")
print("\nGenerated files:")
print("  - 01_length_distribution.png")
print("  - 02_success_vs_errors.png")
print("  - 03_input_vs_output_length.png")
print("  - 04_top_words.png")
print("  - 05_comprehensive_summary.png")
print("  - analysis_statistics.txt")
print("\n" + "="*80)
