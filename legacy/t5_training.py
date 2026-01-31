import pandas as pd
import numpy as np
import torch
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from transformers import (
    T5Tokenizer,
    T5ForConditionalGeneration,
    Trainer,
    TrainingArguments,
    DataCollatorForSeq2Seq,
    EarlyStoppingCallback
)
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import gc
import warnings
from collections import Counter
from tqdm import tqdm
import json
import sys

warnings.filterwarnings('ignore')

# ============================================================================
# HUGGINGFACE AUTHENTICATION & MODEL LOADING
# ============================================================================

def try_load_model(model_names=['t5-medium', 't5-small', 't5-base']):
    """
    Try to load T5 models in order of preference.
    Returns tokenizer, model, and model_name
    """
    print(f"\n{'MODEL LOADING':-^100}")
    
    for model_name in model_names:
        print(f"\nAttempting to load: {model_name}")
        try:
            # Try loading with token from environment
            token = os.environ.get('HF_TOKEN', None)
            
            tokenizer = T5Tokenizer.from_pretrained(
                model_name, 
                legacy=False,
                token=token
            )
            model = T5ForConditionalGeneration.from_pretrained(
                model_name,
                token=token
            )
            
            print(f" Successfully loaded: {model_name}")
            print(f" Parameters: {model.num_parameters():,}")
            return tokenizer, model, model_name
            
        except Exception as e:
            print(f" Failed to load {model_name}: {str(e)[:100]}")
            continue
    
    print("   python -c \"from transformers import T5Tokenizer, T5ForConditionalGeneration;")
    print("   T5Tokenizer.from_pretrained('t5-small').save_pretrained('./t5-small');")
    print("   T5ForConditionalGeneration.from_pretrained('t5-small').save_pretrained('./t5-small')\"")
    print("\n   Then update MODEL_NAME to './t5-small'")
    print("="*100)
    sys.exit(1)

# ============================================================================
# CONFIGURATION - CPU OPTIMIZED
# ============================================================================

print("="*100)
print("UK PROPERTY CLASSIFIER & TERM PARSER - T5 CPU-OPTIMIZED TRAINING")
print("="*100)

# Input files
PROPERTY_FILE = '3_NEW_records_w_postcode_enriched.csv'
TERM_FILE = 'Term_Parsed_Results.csv'

# Model will be determined after loading
OUTPUT_DIR = './'
MODEL_SAVE_PATH = './'
CHECKPOINT_DIR = './'

# CPU-Optimized Training Parameters (will adjust based on model size)
BASE_BATCH_SIZE = 4
GRADIENT_ACCUMULATION = 8

EPOCHS = 10
LEARNING_RATE = 3e-5
WARMUP_STEPS = 200
WEIGHT_DECAY = 0.01

# Tokenization limits
MAX_INPUT_LENGTH = 128
MAX_OUTPUT_LENGTH = 64

# Data splits
VALIDATION_SPLIT = 0.15
TEST_SPLIT = 0.15

# CPU optimizations
NUM_WORKERS = 0
EVAL_ACCUMULATION_STEPS = 10
DATALOADER_PIN_MEMORY = False

# Early stopping
EARLY_STOPPING_PATIENCE = 3

# Reporting
REPORT_FILE = 'training_report_cpu.txt'
PLOTS_DIR = './training_plots'
PREDICTIONS_FILE = 'test_predictions.csv'

# Create directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(PLOTS_DIR, exist_ok=True)

# ============================================================================
# DATA LOADING & PREPROCESSING
# ============================================================================

print(f"\n{'DATA LOADING':-^100}")

# Load property data
try:
    prop_df = pd.read_csv(PROPERTY_FILE, dtype=str, low_memory=False)
    print(f" Property data loaded: {len(prop_df):,} records")
    print(f"  Columns: {list(prop_df.columns)}")
except FileNotFoundError:
    print(f" ERROR: File not found: {PROPERTY_FILE}")
    sys.exit(1)

# Load term data
try:
    term_df = pd.read_csv(TERM_FILE, dtype=str, low_memory=False)
    print(f" Term data loaded: {len(term_df):,} records")
    print(f"  Columns: {list(term_df.columns)}")
except FileNotFoundError:
    print(f" ERROR: File not found: {TERM_FILE}")
    sys.exit(1)

# ============================================================================
# DATA PREPARATION - PROPERTY CLASSIFICATION
# ============================================================================

print(f"\n{'PROPERTY DATA PREPARATION':-^100}")

# Identify property description column
prop_desc_col = None
for col in ['Register Property Description', 'Associated_Property_Description', 'Property_Description']:
    if col in prop_df.columns:
        prop_desc_col = col
        break

if prop_desc_col is None:
    print(" ERROR: Could not find property description column")
    print(f"  Available columns: {list(prop_df.columns)}")
    sys.exit(1)

print(f" Using property description column: '{prop_desc_col}'")

# Identify property type column
prop_type_col = None
for col in ['property_type', 'Property_Type', 'Primary_Category']:
    if col in prop_df.columns:
        prop_type_col = col
        break

if prop_type_col is None:
    print(" ERROR: Could not find property type column")
    print(f"  Available columns: {list(prop_df.columns)}")
    sys.exit(1)

print(f" Using property type column: '{prop_type_col}'")

# Filter valid property records (exclude "not found")
prop_valid = prop_df[
    (prop_df[prop_desc_col].notna()) &
    (prop_df[prop_desc_col].str.strip() != '') &
    (prop_df[prop_type_col].notna()) &
    (prop_df[prop_type_col].str.strip() != '') &
    (prop_df[prop_type_col].str.lower() != 'not found')
].copy()

print(f" Valid property records: {len(prop_valid):,}")

# Property type distribution
prop_type_dist = prop_valid[prop_type_col].value_counts()
print(f"\nProperty Type Distribution:")
for ptype, count in prop_type_dist.head(10).items():
    print(f"  {ptype}: {count:,} ({count/len(prop_valid)*100:.1f}%)")

if len(prop_valid) == 0:
    print("\n ERROR: No valid property records after filtering")
    sys.exit(1)

# ============================================================================
# DATA PREPARATION - TERM PARSING
# ============================================================================

print(f"\n{'TERM DATA PREPARATION':-^100}")

# Identify term columns
term_orig_col = None
for col in ['Term_Original', 'Term', 'Original_Term']:
    if col in term_df.columns:
        term_orig_col = col
        break

if term_orig_col is None:
    print(" ERROR: Could not find term original column")
    print(f"  Available columns: {list(term_df.columns)}")
    sys.exit(1)

print(f" Using term original column: '{term_orig_col}'")

# Required output columns
required_cols = ['Start_Date', 'End_Date', 'Tenor']
missing_cols = [col for col in required_cols if col not in term_df.columns]

if missing_cols:
    print(f" ERROR: Missing required columns: {missing_cols}")
    print(f"  Available columns: {list(term_df.columns)}")
    sys.exit(1)

print(f" All required columns found: {required_cols}")

# Filter valid term records
term_valid = term_df[
    (term_df[term_orig_col].notna()) &
    (term_df[term_orig_col].str.strip() != '') &
    (term_df['Start_Date'].notna()) &
    (term_df['End_Date'].notna()) &
    (term_df['Tenor'].notna())
].copy()

print(f" Valid term records: {len(term_valid):,}")

# Sample term data
print("\nSample Term Records:")
for i in range(min(3, len(term_valid))):
    row = term_valid.iloc[i]
    print(f"\n  Original: {row[term_orig_col][:80]}")
    print(f"  Start: {row['Start_Date']}, End: {row['End_Date']}, Tenor: {row['Tenor']}")

if len(term_valid) == 0:
    print("\n ERROR: No valid term records after filtering")
    sys.exit(1)

# ============================================================================
# CREATE TRAINING EXAMPLES
# ============================================================================

print(f"\n{'CREATING TRAINING EXAMPLES':-^100}")

def create_property_output(row):
    """Format: property_type"""
    return str(row[prop_type_col]).strip()

def create_term_output(row):
    """Format: start_date|end_date|tenor"""
    start = str(row['Start_Date']).strip()
    end = str(row['End_Date']).strip()
    tenor = str(row['Tenor']).strip()
    return f"{start}|{end}|{tenor}"

# Generate property examples
property_examples = []
for idx, row in prop_valid.iterrows():
    desc = str(row[prop_desc_col]).strip()
    if len(desc) > 0:
        property_examples.append({
            'input': f"classify property: {desc}",
            'output': create_property_output(row),
            'task': 'property',
            'original_desc': desc,
            'property_type': row[prop_type_col]
        })

print(f" Property examples created: {len(property_examples):,}")

# Generate term examples
term_examples = []
for idx, row in term_valid.iterrows():
    term_text = str(row[term_orig_col]).strip()
    if len(term_text) > 0:
        term_examples.append({
            'input': f"parse term: {term_text}",
            'output': create_term_output(row),
            'task': 'term',
            'original_term': term_text,
            'start_date': row['Start_Date'],
            'end_date': row['End_Date'],
            'tenor': row['Tenor']
        })

print(f" Term examples created: {len(term_examples):,}")

# Combine datasets
all_examples = property_examples + term_examples
training_data = pd.DataFrame(all_examples)

print(f"\n Total training examples: {len(training_data):,}")
print(f"  Property classification: {len(property_examples):,}")
print(f"  Term parsing: {len(term_examples):,}")

# ============================================================================
# DATA SPLITTING
# ============================================================================

print(f"\n{'DATA SPLITTING':-^100}")

# Stratified split
train_val, test_data = train_test_split(
    training_data,
    test_size=TEST_SPLIT,
    random_state=42,
    stratify=training_data['task']
)

train_data, val_data = train_test_split(
    train_val,
    test_size=VALIDATION_SPLIT/(1-TEST_SPLIT),
    random_state=42,
    stratify=train_val['task']
)

print(f" Training set: {len(train_data):,} examples")
print(f"  Property: {len(train_data[train_data['task']=='property']):,}")
print(f"  Term: {len(train_data[train_data['task']=='term']):,}")

print(f"\n Validation set: {len(val_data):,} examples")
print(f"  Property: {len(val_data[val_data['task']=='property']):,}")
print(f"  Term: {len(val_data[val_data['task']=='term']):,}")

print(f"\n Test set: {len(test_data):,} examples")
print(f"  Property: {len(test_data[test_data['task']=='property']):,}")
print(f"  Term: {len(test_data[test_data['task']=='term']):,}")

# ============================================================================
# MODEL LOADING WITH FALLBACK
# ============================================================================

tokenizer, model, MODEL_NAME = try_load_model(['t5-medium', 't5-small', 't5-base'])

# Adjust batch size based on model
model_params = model.num_parameters()
if model_params > 500_000_000:  # t5-medium or larger
    BATCH_SIZE = 2
    GRADIENT_ACCUMULATION = 16
elif model_params > 200_000_000:  # t5-base
    BATCH_SIZE = 4
    GRADIENT_ACCUMULATION = 8
else:  # t5-small
    BATCH_SIZE = 8
    GRADIENT_ACCUMULATION = 4

EFFECTIVE_BATCH = BATCH_SIZE * GRADIENT_ACCUMULATION

print(f"\n{'ADJUSTED CONFIGURATION':-^100}")
print(f"Model size: {model_params:,} parameters")
print(f"Batch Size: {BATCH_SIZE}")
print(f"Gradient Accumulation: {GRADIENT_ACCUMULATION}")
print(f"Effective Batch Size: {EFFECTIVE_BATCH}")

# Memory optimization
model.config.use_cache = False
gc.collect()

# ============================================================================
# DATASET CLASS - PRE-TOKENIZED
# ============================================================================

class OptimizedDataset(Dataset):
    """Pre-tokenized dataset for CPU efficiency"""
    
    def __init__(self, data, tokenizer, max_input_len, max_output_len, desc="Dataset"):
        print(f"\n  Pre-tokenizing {desc} ({len(data)} examples)...")
        self.examples = []
        
        for idx, row in tqdm(data.iterrows(), total=len(data), desc=f"  {desc}"):
            # Tokenize input
            input_enc = tokenizer(
                row['input'],
                max_length=max_input_len,
                padding='max_length',
                truncation=True,
                return_tensors='pt'
            )
            
            # Tokenize output
            output_enc = tokenizer(
                row['output'],
                max_length=max_output_len,
                padding='max_length',
                truncation=True,
                return_tensors='pt'
            )
            
            self.examples.append({
                'input_ids': input_enc['input_ids'].squeeze(),
                'attention_mask': input_enc['attention_mask'].squeeze(),
                'labels': output_enc['input_ids'].squeeze(),
                'task': row['task']
            })
        
        print(f"   {desc} ready")
    
    def __len__(self):
        return len(self.examples)
    
    def __getitem__(self, idx):
        return self.examples[idx]

# Create datasets
print(f"\n{'CREATING DATASETS':-^100}")

train_dataset = OptimizedDataset(
    train_data, tokenizer, MAX_INPUT_LENGTH, MAX_OUTPUT_LENGTH, "Train"
)
val_dataset = OptimizedDataset(
    val_data, tokenizer, MAX_INPUT_LENGTH, MAX_OUTPUT_LENGTH, "Validation"
)
test_dataset = OptimizedDataset(
    test_data, tokenizer, MAX_INPUT_LENGTH, MAX_OUTPUT_LENGTH, "Test"
)

print(f"\n All datasets ready")

# ============================================================================
# TRAINING CONFIGURATION
# ============================================================================

print(f"\n{'TRAINING CONFIGURATION':-^100}")

training_args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    num_train_epochs=EPOCHS,
    
    # Batch configuration
    per_device_train_batch_size=BATCH_SIZE,
    per_device_eval_batch_size=BATCH_SIZE,
    gradient_accumulation_steps=GRADIENT_ACCUMULATION,
    
    # Optimization
    learning_rate=LEARNING_RATE,
    weight_decay=WEIGHT_DECAY,
    warmup_steps=WARMUP_STEPS,
    lr_scheduler_type='linear',
    
    # Evaluation
    eval_strategy='epoch',
    save_strategy='epoch',
    load_best_model_at_end=True,
    metric_for_best_model='eval_loss',
    greater_is_better=False,
    
    # Logging
    logging_steps=50,
    logging_first_step=True,
    save_total_limit=3,
    
    # CPU optimizations
    fp16=False,
    bf16=False,
    dataloader_num_workers=NUM_WORKERS,
    dataloader_pin_memory=DATALOADER_PIN_MEMORY,
    eval_accumulation_steps=EVAL_ACCUMULATION_STEPS,
    
    # Other
    optim='adamw_torch',
    report_to='none',
    disable_tqdm=False,
    seed=42,
    data_seed=42
)

print(f" Training arguments configured")
print(f"  Device: {training_args.device}")
print(f"  Effective batch size: {EFFECTIVE_BATCH}")
print(f"  Total training steps: {len(train_dataset) // EFFECTIVE_BATCH * EPOCHS}")

# Data collator
data_collator = DataCollatorForSeq2Seq(
    tokenizer=tokenizer,
    model=model,
    padding=True,
    pad_to_multiple_of=8
)

# Callbacks
callbacks = [
    EarlyStoppingCallback(early_stopping_patience=EARLY_STOPPING_PATIENCE)
]

# Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    data_collator=data_collator,
    tokenizer=tokenizer,
    callbacks=callbacks
)

print(f" Trainer initialized with early stopping (patience={EARLY_STOPPING_PATIENCE})")

# ============================================================================
# TRAINING
# ============================================================================

print(f"\n{'TRAINING':-^100}")
print(f"Starting training at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Training on {len(train_dataset):,} examples for {EPOCHS} epochs")
print(f"Model: {MODEL_NAME}")
print(f"This may take a while on CPU...\n")

start_time = datetime.now()

try:
    train_result = trainer.train()
    training_completed = True
except KeyboardInterrupt:
    print("\n⚠ Training interrupted by user")
    training_completed = False
except Exception as e:
    print(f"\n Training error: {e}")
    import traceback
    traceback.print_exc()
    training_completed = False
    sys.exit(1)

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print(f"\n{'TRAINING COMPLETE':-^100}")
print(f"Duration: {duration/60:.1f} minutes ({duration/3600:.2f} hours)")
print(f"Final training loss: {train_result.training_loss:.4f}")
print(f"Samples/second: {len(train_dataset) * EPOCHS / duration:.2f}")

# ============================================================================
# EVALUATION
# ============================================================================

print(f"\n{'EVALUATION':-^100}")

# Validation metrics
print("\nEvaluating on validation set...")
val_metrics = trainer.evaluate(val_dataset)
print(f" Validation loss: {val_metrics['eval_loss']:.4f}")

# Test metrics
print("\nEvaluating on test set...")
test_metrics = trainer.evaluate(test_dataset)
print(f" Test loss: {test_metrics['eval_loss']:.4f}")

# ============================================================================
# SAVE MODEL
# ============================================================================

print(f"\n{'SAVING MODEL':-^100}")

model.save_pretrained(MODEL_SAVE_PATH)
tokenizer.save_pretrained(MODEL_SAVE_PATH)

model_size_mb = sum(
    os.path.getsize(os.path.join(MODEL_SAVE_PATH, f))
    for f in os.listdir(MODEL_SAVE_PATH)
    if os.path.isfile(os.path.join(MODEL_SAVE_PATH, f))
) / (1024 * 1024)

print(f" Model saved to: {MODEL_SAVE_PATH}")
print(f" Model size: {model_size_mb:.1f} MB")

# ============================================================================
# PREDICTION FUNCTIONS
# ============================================================================

def predict(text, max_length=MAX_OUTPUT_LENGTH):
    """Generate prediction"""
    model.eval()
    with torch.no_grad():
        inputs = tokenizer(text, return_tensors='pt').input_ids
        outputs = model.generate(
            inputs,
            max_length=max_length,
            num_beams=4,
            early_stopping=True,
            no_repeat_ngram_size=2
        )
        return tokenizer.decode(outputs[0], skip_special_tokens=True)

def parse_term_output(text):
    """Parse pipe-delimited term output"""
    parts = text.split('|')
    return {
        'start_date': parts[0].strip() if len(parts) > 0 else 'Error',
        'end_date': parts[1].strip() if len(parts) > 1 else 'Error',
        'tenor': parts[2].strip() if len(parts) > 2 else 'Error'
    }

# ============================================================================
# DETAILED PREDICTIONS & ANALYSIS
# ============================================================================

print(f"\n{'PREDICTION ANALYSIS':-^100}")

# Sample predictions
print("\n" + "="*100)
print("PROPERTY CLASSIFICATION SAMPLES")
print("="*100)

prop_correct = 0
prop_total = 0

for i in range(min(10, len(test_data))):
    row = test_data.iloc[i]
    if row['task'] == 'property':
        prop_total += 1
        pred = predict(row['input'])
        expected = row['output']
        
        match = pred.strip().lower() == expected.strip().lower()
        if match:
            prop_correct += 1
        
        print(f"\n{'='*100}")
        print(f"Example {prop_total}")
        print(f"{'='*100}")
        print(f"Input: {row['original_desc'][:200]}")
        print(f"Expected: {expected}")
        print(f"Predicted: {pred}")
        print(f"Match: {' CORRECT' if match else ' INCORRECT'}")

print(f"\n{'='*100}")
print("TERM PARSING SAMPLES")
print(f"{'='*100}")

term_correct = 0
term_total = 0
term_field_correct = {'start_date': 0, 'end_date': 0, 'tenor': 0}

for i in range(min(10, len(test_data))):
    row = test_data.iloc[i]
    if row['task'] == 'term':
        term_total += 1
        pred = predict(row['input'])
        expected = row['output']
        
        pred_parsed = parse_term_output(pred)
        exp_parsed = parse_term_output(expected)
        
        # Exact match
        exact_match = pred.strip() == expected.strip()
        if exact_match:
            term_correct += 1
        
        # Field-level accuracy
        for field in ['start_date', 'end_date', 'tenor']:
            if pred_parsed[field].lower() == exp_parsed[field].lower():
                term_field_correct[field] += 1
        
        print(f"\n{'='*100}")
        print(f"Example {term_total}")
        print(f"{'='*100}")
        print(f"Input: {row['original_term']}")
        print(f"\nExpected:")
        print(f"  Start: {exp_parsed['start_date']}")
        print(f"  End: {exp_parsed['end_date']}")
        print(f"  Tenor: {exp_parsed['tenor']}")
        print(f"\nPredicted:")
        print(f"  Start: {pred_parsed['start_date']}")
        print(f"  End: {pred_parsed['end_date']}")
        print(f"  Tenor: {pred_parsed['tenor']}")
        print(f"\nMatch: {' EXACT' if exact_match else ' PARTIAL/INCORRECT'}")

# ============================================================================
# COMPREHENSIVE ACCURACY EVALUATION
# ============================================================================

print(f"\n{'COMPREHENSIVE ACCURACY EVALUATION':-^100}")
print("Evaluating all test samples (this may take a few minutes)...\n")

all_predictions = []

# Property evaluation
print("Evaluating Property Classification...")
prop_predictions = []
prop_actuals = []

for idx, row in tqdm(test_data[test_data['task']=='property'].iterrows(), 
                      total=len(test_data[test_data['task']=='property']),
                      desc="Property"):
    pred = predict(row['input'])
    prop_predictions.append(pred.strip().lower())
    prop_actuals.append(row['output'].strip().lower())
    
    all_predictions.append({
        'task': 'property',
        'input': row['original_desc'],
        'expected': row['output'],
        'predicted': pred,
        'correct': pred.strip().lower() == row['output'].strip().lower()
    })

prop_accuracy = accuracy_score(prop_actuals, prop_predictions)

# Term evaluation
print("\nEvaluating Term Parsing...")
term_exact_correct = 0
term_field_accuracy = {'start_date': 0, 'end_date': 0, 'tenor': 0}
term_all_fields = 0

for idx, row in tqdm(test_data[test_data['task']=='term'].iterrows(),
                      total=len(test_data[test_data['task']=='term']),
                      desc="Term"):
    pred = predict(row['input'])
    expected = row['output']
    
    pred_parsed = parse_term_output(pred)
    exp_parsed = parse_term_output(expected)
    
    # Exact match
    exact = pred.strip() == expected.strip()
    if exact:
        term_exact_correct += 1
    
    # Field-level
    all_correct = True
    for field in ['start_date', 'end_date', 'tenor']:
        if pred_parsed[field].lower() == exp_parsed[field].lower():
            term_field_accuracy[field] += 1
        else:
            all_correct = False
    
    if all_correct:
        term_all_fields += 1
    
    all_predictions.append({
        'task': 'term',
        'input': row['original_term'],
        'expected': expected,
        'predicted': pred,
        'correct': exact,
        'start_correct': pred_parsed['start_date'].lower() == exp_parsed['start_date'].lower(),
        'end_correct': pred_parsed['end_date'].lower() == exp_parsed['end_date'].lower(),
        'tenor_correct': pred_parsed['tenor'].lower() == exp_parsed['tenor'].lower()
    })

term_count = len(test_data[test_data['task']=='term'])

# Save predictions
pred_df = pd.DataFrame(all_predictions)
pred_df.to_csv(PREDICTIONS_FILE, index=False)
print(f"\n Predictions saved to: {PREDICTIONS_FILE}")

# ============================================================================
# RESULTS SUMMARY
# ============================================================================

print(f"\n{'RESULTS SUMMARY':-^100}")

print(f"\nPROPERTY CLASSIFICATION:")
print(f"  Total samples: {len(prop_actuals)}")
print(f"  Exact match accuracy: {prop_accuracy*100:.2f}%")
print(f"  Correct predictions: {int(prop_accuracy*len(prop_actuals))}/{len(prop_actuals)}")

print(f"\nTERM PARSING:")
print(f"  Total samples: {term_count}")
print(f"  Exact match accuracy: {term_exact_correct/term_count*100:.2f}%")
print(f"  All fields correct: {term_all_fields/term_count*100:.2f}%")
print(f"\n  Field-level accuracy:")
print(f"    Start Date: {term_field_accuracy['start_date']/term_count*100:.2f}%")
print(f"    End Date: {term_field_accuracy['end_date']/term_count*100:.2f}%")
print(f"    Tenor: {term_field_accuracy['tenor']/term_count*100:.2f}%")

# ============================================================================
# VISUALIZATION
# ============================================================================

print(f"\n{'CREATING VISUALIZATIONS':-^100}")

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 10)

# Figure 1: Training Metrics
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle(f'{MODEL_NAME.upper()} Training Analysis', fontsize=16, fontweight='bold')

# Loss curves (if available in trainer.state.log_history)
if hasattr(trainer.state, 'log_history'):
    train_logs = [x for x in trainer.state.log_history if 'loss' in x]
    eval_logs = [x for x in trainer.state.log_history if 'eval_loss' in x]
    
    if train_logs:
        steps = [x['step'] for x in train_logs]
        losses = [x['loss'] for x in train_logs]
        axes[0, 0].plot(steps, losses, 'b-', linewidth=2, label='Training Loss')
        axes[0, 0].set_title('Training Loss Over Time', fontweight='bold')
        axes[0, 0].set_xlabel('Steps')
        axes[0, 0].set_ylabel('Loss')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
    
    if eval_logs:
        epochs = list(range(1, len(eval_logs)+1))
        eval_losses = [x['eval_loss'] for x in eval_logs]
        axes[0, 1].plot(epochs, eval_losses, 'r-', linewidth=2, marker='o', markersize=8, label='Validation Loss')
        axes[0, 1].set_title('Validation Loss by Epoch', fontweight='bold')
        axes[0, 1].set_xlabel('Epoch')
        axes[0, 1].set_ylabel('Loss')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)

# Accuracy comparison
tasks = ['Property\nClassification', 'Term Parsing\n(Exact)', 'Term Parsing\n(All Fields)']
accuracies = [
    prop_accuracy * 100,
    (term_exact_correct / term_count) * 100,
    (term_all_fields / term_count) * 100
]
colors = ['#2ecc71', '#e74c3c', '#3498db']

bars = axes[1, 0].bar(tasks, accuracies, color=colors, alpha=0.7, edgecolor='black', linewidth=1.5)
axes[1, 0].set_title('Model Accuracy by Task', fontweight='bold')
axes[1, 0].set_ylabel('Accuracy (%)')
axes[1, 0].set_ylim(0, 100)
axes[1, 0].axhline(y=50, color='gray', linestyle='--', alpha=0.5, label='50% baseline')
axes[1, 0].legend()
axes[1, 0].grid(True, alpha=0.3, axis='y')

# Add value labels on bars
for bar, acc in zip(bars, accuracies):
    height = bar.get_height()
    axes[1, 0].text(bar.get_x() + bar.get_width()/2., height,
                    f'{acc:.1f}%',
                    ha='center', va='bottom', fontweight='bold', fontsize=11)

# Field-level accuracy for terms
fields = ['Start Date', 'End Date', 'Tenor']
field_accs = [
    term_field_accuracy['start_date'] / term_count * 100,
    term_field_accuracy['end_date'] / term_count * 100,
    term_field_accuracy['tenor'] / term_count * 100
]
field_colors = ['#1abc9c', '#9b59b6', '#f39c12']

bars2 = axes[1, 1].bar(fields, field_accs, color=field_colors, alpha=0.7, edgecolor='black', linewidth=1.5)
axes[1, 1].set_title('Term Parsing: Field-Level Accuracy', fontweight='bold')
axes[1, 1].set_ylabel('Accuracy (%)')
axes[1, 1].set_ylim(0, 100)
axes[1, 1].grid(True, alpha=0.3, axis='y')

for bar, acc in zip(bars2, field_accs):
    height = bar.get_height()
    axes[1, 1].text(bar.get_x() + bar.get_width()/2., height,
                    f'{acc:.1f}%',
                    ha='center', va='bottom', fontweight='bold', fontsize=11)

plt.tight_layout()
plot_file = os.path.join(PLOTS_DIR, 'training_analysis.png')
plt.savefig(plot_file, dpi=300, bbox_inches='tight')
print(f" Saved: {plot_file}")
plt.close()

# Figure 2: Data Distribution
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle('Dataset Distribution', fontsize=16, fontweight='bold')

# Task distribution
task_counts = training_data['task'].value_counts()
colors_pie = ['#3498db', '#e74c3c']
axes[0].pie(task_counts.values, labels=task_counts.index, autopct='%1.1f%%',
            colors=colors_pie, startangle=90, textprops={'fontsize': 12, 'fontweight': 'bold'})
axes[0].set_title('Training Examples by Task', fontweight='bold')

# Property type distribution (top 15)
if prop_type_col and len(prop_valid) > 0:
    prop_dist = prop_valid[prop_type_col].value_counts().head(15)
    axes[1].barh(range(len(prop_dist)), prop_dist.values, color='steelblue', alpha=0.7)
    axes[1].set_yticks(range(len(prop_dist)))
    axes[1].set_yticklabels(prop_dist.index)
    axes[1].set_xlabel('Count')
    axes[1].set_title(f'Top 15 Property Types', fontweight='bold')
    axes[1].grid(True, alpha=0.3, axis='x')
    
    for i, v in enumerate(prop_dist.values):
        axes[1].text(v, i, f' {v:,}', va='center', fontweight='bold')

plt.tight_layout()
plot_file2 = os.path.join(PLOTS_DIR, 'data_distribution.png')
plt.savefig(plot_file2, dpi=300, bbox_inches='tight')
print(f" Saved: {plot_file2}")
plt.close()

# ============================================================================
# GENERATE COMPREHENSIVE REPORT
# ============================================================================

print(f"\n{'GENERATING REPORT':-^100}")

report = f"""
{'='*100}
UK PROPERTY CLASSIFIER & TERM PARSER - TRAINING REPORT
{'='*100}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{'MODEL CONFIGURATION':-^100}
Model: {MODEL_NAME}
Parameters: {model.num_parameters():,}
Trainable Parameters: {sum(p.numel() for p in model.parameters() if p.requires_grad):,}
Model Size: {model_size_mb:.1f} MB

{'TRAINING CONFIGURATION':-^100}
Batch Size: {BATCH_SIZE}
Gradient Accumulation Steps: {GRADIENT_ACCUMULATION}
Effective Batch Size: {EFFECTIVE_BATCH}
Epochs: {EPOCHS}
Learning Rate: {LEARNING_RATE}
Warmup Steps: {WARMUP_STEPS}
Weight Decay: {WEIGHT_DECAY}

Max Input Length: {MAX_INPUT_LENGTH}
Max Output Length: {MAX_OUTPUT_LENGTH}

{'DATASET STATISTICS':-^100}
Property File: {PROPERTY_FILE}
  Total records: {len(prop_df):,}
  Valid records: {len(prop_valid):,}
  Property description column: {prop_desc_col}
  Property type column: {prop_type_col}

Term File: {TERM_FILE}
  Total records: {len(term_df):,}
  Valid records: {len(term_valid):,}
  Term original column: {term_orig_col}

Training Examples: {len(training_data):,}
  Property classification: {len(property_examples):,}
  Term parsing: {len(term_examples):,}

Data Splits:
  Training: {len(train_data):,} ({len(train_data)/len(training_data)*100:.1f}%)
  Validation: {len(val_data):,} ({len(val_data)/len(training_data)*100:.1f}%)
  Test: {len(test_data):,} ({len(test_data)/len(training_data)*100:.1f}%)

{'TRAINING RESULTS':-^100}
Duration: {duration/60:.1f} minutes ({duration/3600:.2f} hours)
Samples/Second: {len(train_dataset) * EPOCHS / duration:.2f}

Final Training Loss: {train_result.training_loss:.4f}
Validation Loss: {val_metrics['eval_loss']:.4f}
Test Loss: {test_metrics['eval_loss']:.4f}

{'ACCURACY RESULTS':-^100}

PROPERTY CLASSIFICATION:
  Total test samples: {len(prop_actuals)}
  Exact match accuracy: {prop_accuracy*100:.2f}%
  Correct predictions: {int(prop_accuracy*len(prop_actuals))}/{len(prop_actuals)}

TERM PARSING:
  Total test samples: {term_count}
  Exact match accuracy: {term_exact_correct/term_count*100:.2f}%
  All fields correct: {term_all_fields/term_count*100:.2f}%
  
  Field-level Accuracy:
    Start Date: {term_field_accuracy['start_date']/term_count*100:.2f}%
    End Date: {term_field_accuracy['end_date']/term_count*100:.2f}%
    Tenor: {term_field_accuracy['tenor']/term_count*100:.2f}%

{'OUTPUT FORMATS':-^100}
Property Classification:
  Input: "classify property: <description>"
  Output: "<property_type>"
  Example: "Residential"

Term Parsing:
  Input: "parse term: <term_text>"
  Output: "<start_date>|<end_date>|<tenor>"
  Example: "01/03/2001|Not specified|125 years"

{'FILES GENERATED':-^100}
Model: {MODEL_SAVE_PATH}
Predictions: {PREDICTIONS_FILE}
Plots: {PLOTS_DIR}/
Report: {REPORT_FILE}

{'PROPERTY TYPE DISTRIBUTION':-^100}
"""

if prop_type_col and len(prop_valid) > 0:
    for ptype, count in prop_valid[prop_type_col].value_counts().items():
        report += f"{ptype}: {count:,} ({count/len(prop_valid)*100:.1f}%)\n"

report += f"""
{'='*100}
END OF REPORT
{'='*100}
"""

with open(REPORT_FILE, 'w') as f:
    f.write(report)

print(f" Report saved: {REPORT_FILE}")

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print(f"\n{'='*100}")
print("TRAINING COMPLETE - SUMMARY")
print(f"{'='*100}")
print(f"\n Model: {MODEL_NAME}")
print(f" Training time: {duration/60:.1f} minutes ({duration/3600:.2f} hours)")
print(f" Model saved: {MODEL_SAVE_PATH}")
print(f"\n Property classification accuracy: {prop_accuracy*100:.2f}%")
print(f" Term parsing exact match: {term_exact_correct/term_count*100:.2f}%")
print(f" Term parsing all fields: {term_all_fields/term_count*100:.2f}%")
print(f"\n Report: {REPORT_FILE}")
print(f" Predictions: {PREDICTIONS_FILE}")
print(f" Plots: {PLOTS_DIR}/")
print(f"\n{'='*100}")
print("All files generated successfully!")
print(f"{'='*100}\n")
