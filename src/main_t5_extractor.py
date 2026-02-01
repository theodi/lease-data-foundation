"""
T5-based extractor for lease records where regex extraction failed.

Processes records with regex_is_valid=False using a fine-tuned T5 model.
Implements efficient batch processing to handle ~70,000+ records with slow
T5 inference (3-5 seconds per single call).

Key optimizations:
- Batch inference: Process multiple inputs in single forward pass
- Bulk MongoDB updates: Reduce database round-trips
- Progress checkpointing: Resume from interruptions
- GPU acceleration: Use CUDA if available
"""
import os.path
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
import re
from pathlib import Path

import torch
from pymongo import UpdateOne
from tqdm import tqdm
from transformers import T5Tokenizer, T5ForConditionalGeneration
from dateutil.relativedelta import relativedelta

from utils.mongo_client import MongoDBClient
from utils.lease_term_validator import is_lease_term_valid

# Configuration
CONNECTION_STRING = "mongodb://localhost:27017"
DATABASE_NAME = "leases"
COLLECTION_NAME = "leases"
TERM_FIELD = "term"
DOL_FIELD = "dol"

# T5 Model settings
CURRENT_FOLDER = Path(__file__).parent
MODEL_PATH = os.path.join(CURRENT_FOLDER, "../t5_model/trained_t5")

# Batch processing settings - tuned for T5 inference
# Larger batches = better GPU utilization, but more memory
T5_BATCH_SIZE = 32  # Number of records to process in single T5 forward pass
DB_BATCH_SIZE = 500  # Number of updates to accumulate before bulk write
MAX_LENGTH = 64  # Max token length for T5


class BatchT5Extractor:
    """
    Batch-optimized T5 extractor for lease terms.

    Processes multiple records in a single forward pass for efficiency.
    """

    def __init__(self, model_path: str = MODEL_PATH):
        """Initialize the batch T5 extractor with model loading."""
        print("Loading T5 model...")
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"Using device: {self.device}")

        self.tokenizer = T5Tokenizer.from_pretrained(model_path, legacy=False)
        self.model = T5ForConditionalGeneration.from_pretrained(model_path)
        self.model.to(self.device)
        self.model.eval()

        self._max_length = MAX_LENGTH
        print("T5 model loaded successfully.")

    def extract_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract lease terms from a batch of records.

        Args:
            records: List of MongoDB documents with 'term' field

        Returns:
            List of extraction results (same order as input)
        """
        if not records:
            return []

        # Prepare inputs
        input_texts = [f"parse lease: {r.get(TERM_FIELD, '')}" for r in records]

        # Tokenize batch
        inputs = self.tokenizer(
            input_texts,
            max_length=self._max_length,
            padding=True,
            truncation=True,
            return_tensors='pt'
        )
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        # Generate outputs in batch
        with torch.no_grad():
            output_ids = self.model.generate(
                inputs['input_ids'],
                attention_mask=inputs['attention_mask'],
                max_length=self._max_length,
                num_beams=4,
                early_stopping=True
            )

        # Decode outputs
        raw_outputs = self.tokenizer.batch_decode(output_ids, skip_special_tokens=True)

        # Parse each output
        results = []
        for i, (record, raw_output) in enumerate(zip(records, raw_outputs)):
            dol = record.get(DOL_FIELD)
            parsed = self._parse_and_validate(raw_output, dol)
            results.append(parsed)

        return results

    def _parse_and_validate(self, raw_output: str, dol: Optional[str] = None) -> Dict[str, Any]:
        """
        Parse T5 output and validate the results.

        Args:
            raw_output: Raw string output from T5 model
            dol: Optional date of lease

        Returns:
            Dictionary with extraction results and validation status
        """
        parsed = self._parse_t5_output(raw_output)

        # If start_date not found and dol is provided, use it
        if parsed['start_date'] is None and dol:
            parsed['start_date'] = self._parse_dol_date(dol)
            if parsed['start_date'] and parsed['tenure_years'] and not parsed['expiry_date']:
                parsed['expiry_date'] = parsed['start_date'] + relativedelta(years=parsed['tenure_years'])

        # Check if we have enough data to be valid
        has_valid_data = (
            (parsed['start_date'] is not None and parsed['expiry_date'] is not None) or
            (parsed['start_date'] is not None and parsed['tenure_years'] is not None) or
            (parsed['expiry_date'] is not None and parsed['tenure_years'] is not None)
        )

        if not has_valid_data:
            return {
                "t5_is_valid": False,
                "t5_parse_error": "Insufficient data extracted"
            }

        # Validate the extracted data
        lease_data = {
            'start_date': parsed['start_date'],
            'expiry_date': parsed['expiry_date'],
            'tenure_years': parsed['tenure_years']
        }

        is_valid = is_lease_term_valid(lease_data)

        if is_valid:
            return {
                "t5_is_valid": True,
                "t5_start_date": parsed['start_date'],
                "t5_expiry_date": parsed['expiry_date'],
                "t5_tenure_years": parsed['tenure_years']
            }
        else:
            return {
                "t5_is_valid": False,
                "t5_parse_error": "Validation failed"
            }

    def _parse_t5_output(self, output: str) -> Dict[str, Any]:
        """Parse the T5 model output string into structured data."""
        if not output:
            return {'start_date': None, 'expiry_date': None, 'tenure_years': None}

        output = output.strip()

        # Try to find date patterns (DD/MM/YYYY)
        date_pattern = r'\d{2}/\d{2}/\d{4}'
        dates = re.findall(date_pattern, output)

        start_date = None
        expiry_date = None
        tenure_years = None

        if len(dates) >= 1:
            start_date = self._parse_date(dates[0])
        if len(dates) >= 2:
            expiry_date = self._parse_date(dates[1])

        # Extract tenure from the remaining text
        remaining = re.sub(date_pattern, '', output)
        remaining = remaining.replace('Not specified', '').strip()

        if remaining:
            tenure_years = self._parse_tenure(remaining)

        # If we only have "Not specified" entries, the output might be just a tenure
        if not dates and not start_date and not expiry_date:
            tenure_years = self._parse_tenure(output)

            # Check for special day + year patterns
            special_match = re.search(r'(Christmas|Midsummer|Lady|Michaelmas)(?:\s+Day)?\s+(\d{4})', output, re.IGNORECASE)
            if special_match:
                day_name = special_match.group(1).lower()
                year = int(special_match.group(2))
                special_days = {
                    'christmas': (12, 25),
                    'midsummer': (6, 24),
                    'lady': (3, 25),
                    'michaelmas': (9, 29),
                }
                if day_name in special_days:
                    month, day = special_days[day_name]
                    start_date = datetime(year, month, day)

        # If we have start_date and tenure but no expiry, calculate expiry
        if start_date and tenure_years and not expiry_date:
            expiry_date = start_date + relativedelta(years=tenure_years)

        # If we have start and expiry but no tenure, calculate tenure
        if start_date and expiry_date and not tenure_years:
            delta = relativedelta(expiry_date, start_date)
            tenure_years = delta.years
            if delta.months >= 6:
                tenure_years += 1

        return {
            'start_date': start_date,
            'expiry_date': expiry_date,
            'tenure_years': tenure_years
        }

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse a date string into a datetime object."""
        if not date_str or date_str.lower() in ('not specified', 'residential', ''):
            return None

        date_str = date_str.strip()

        for fmt in ["%d/%m/%Y", "%d.%m.%Y", "%d-%m-%Y"]:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        # Handle special day names
        special_days = {
            'christmas': (12, 25),
            'midsummer': (6, 24),
            'lady day': (3, 25),
            'michaelmas': (9, 29),
        }

        date_str_lower = date_str.lower()
        for day_name, (month, day) in special_days.items():
            if day_name in date_str_lower:
                year_match = re.search(r'\d{4}', date_str)
                if year_match:
                    return datetime(int(year_match.group()), month, day)

        return None

    def _parse_tenure(self, tenure_str: str) -> Optional[int]:
        """Parse a tenure string into years."""
        if not tenure_str or tenure_str.lower() in ('not specified', 'residential', ''):
            return None

        match = re.search(r'(\d+)\s*years?', tenure_str, re.IGNORECASE)
        if match:
            return int(match.group(1))

        return None

    def _parse_dol_date(self, dol: str) -> Optional[datetime]:
        """Parse a date of lease (dol) string into a datetime object."""
        if not dol:
            return None

        dol = dol.strip()
        for fmt in ["%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y"]:
            try:
                return datetime.strptime(dol, fmt)
            except ValueError:
                continue
        return None


def process_t5_records():
    """
    Process all records where regex extraction failed using T5.

    Uses batch processing for efficiency with ~70,000 records.
    """
    # Initialize T5 extractor
    extractor = BatchT5Extractor(MODEL_PATH)

    with MongoDBClient(CONNECTION_STRING, DATABASE_NAME) as mongo:
        collection = mongo.get_collection(COLLECTION_NAME)

        # Query for records where regex failed but have a term field
        # Also skip records already processed by T5
        query_filter = {
            "regex_is_valid": False,
            TERM_FIELD: {"$exists": True, "$ne": ""},
            "t5_is_valid": {"$exists": False}  # Skip already T5-processed records
        }

        # Get total count
        total_count = collection.count_documents(query_filter)
        print(f"Total documents to process with T5: {total_count:,}")

        if total_count == 0:
            print("No documents found for T5 processing.")
            return

        # Estimate processing time
        estimated_time_batch = (total_count / T5_BATCH_SIZE) * 3  # ~3 sec per batch
        print(f"Estimated time (batch processing): {estimated_time_batch / 60:.1f} minutes")

        # Statistics
        stats = {
            "processed": 0,
            "valid": 0,
            "invalid": 0,
            "errors": 0
        }

        start_time = time.time()
        bulk_operations = []
        batch_records = []
        batch_ids = []

        # Use cursor with no_cursor_timeout
        cursor = collection.find(
            query_filter,
            no_cursor_timeout=True,
            batch_size=T5_BATCH_SIZE * 4  # Fetch more from DB to keep pipeline full
        )

        try:
            with tqdm(total=total_count, desc="T5 Processing", unit="docs") as pbar:
                for record in cursor:
                    batch_records.append(record)
                    batch_ids.append(record["_id"])

                    # Process batch when full
                    if len(batch_records) >= T5_BATCH_SIZE:
                        try:
                            results = extractor.extract_batch(batch_records)

                            for doc_id, update_fields in zip(batch_ids, results):
                                bulk_operations.append(
                                    UpdateOne(
                                        {"_id": doc_id},
                                        {"$set": update_fields}
                                    )
                                )

                                if update_fields.get("t5_is_valid"):
                                    stats["valid"] += 1
                                else:
                                    stats["invalid"] += 1

                                stats["processed"] += 1

                        except Exception as e:
                            stats["errors"] += len(batch_records)
                            # Mark all as failed
                            for doc_id in batch_ids:
                                bulk_operations.append(
                                    UpdateOne(
                                        {"_id": doc_id},
                                        {"$set": {"t5_is_valid": False, "t5_parse_error": str(e)}}
                                    )
                                )

                        batch_records = []
                        batch_ids = []
                        pbar.update(T5_BATCH_SIZE)

                        # Bulk write to DB periodically
                        if len(bulk_operations) >= DB_BATCH_SIZE:
                            collection.bulk_write(bulk_operations, ordered=False)
                            bulk_operations = []

                # Process remaining records in last partial batch
                if batch_records:
                    try:
                        results = extractor.extract_batch(batch_records)

                        for doc_id, update_fields in zip(batch_ids, results):
                            bulk_operations.append(
                                UpdateOne(
                                    {"_id": doc_id},
                                    {"$set": update_fields}
                                )
                            )

                            if update_fields.get("t5_is_valid"):
                                stats["valid"] += 1
                            else:
                                stats["invalid"] += 1

                            stats["processed"] += 1

                    except Exception as e:
                        stats["errors"] += len(batch_records)
                        for doc_id in batch_ids:
                            bulk_operations.append(
                                UpdateOne(
                                    {"_id": doc_id},
                                    {"$set": {"t5_is_valid": False, "t5_parse_error": str(e)}}
                                )
                            )

                    pbar.update(len(batch_records))

                # Final bulk write
                if bulk_operations:
                    collection.bulk_write(bulk_operations, ordered=False)

        finally:
            cursor.close()

        # Calculate and print statistics
        elapsed_time = time.time() - start_time
        docs_per_second = stats["processed"] / elapsed_time if elapsed_time > 0 else 0

        print("\n" + "=" * 60)
        print("T5 Processing Complete!")
        print("=" * 60)
        print(f"Total processed:    {stats['processed']:,}")
        print(f"Valid extractions:  {stats['valid']:,} ({100 * stats['valid'] / max(stats['processed'], 1):.1f}%)")
        print(f"Invalid/failed:     {stats['invalid']:,} ({100 * stats['invalid'] / max(stats['processed'], 1):.1f}%)")
        print(f"Errors:             {stats['errors']:,}")
        print(f"Time elapsed:       {elapsed_time:.2f} seconds ({elapsed_time / 60:.1f} minutes)")
        print(f"Processing rate:    {docs_per_second:.1f} docs/second")
        print(f"Batch size used:    {T5_BATCH_SIZE}")
        print("=" * 60)


def main():
    """Main entry point."""
    print("=" * 60)
    print("Lease Data Foundation - T5 Batch Processing")
    print("=" * 60)
    print(f"Database:       {DATABASE_NAME}")
    print(f"Collection:     {COLLECTION_NAME}")
    print(f"T5 Batch size:  {T5_BATCH_SIZE}")
    print(f"DB Batch size:  {DB_BATCH_SIZE}")
    print(f"Model path:     {MODEL_PATH}")
    print("=" * 60)
    print()

    process_t5_records()


if __name__ == "__main__":
    main()

