"""
Lease Data Foundation - Main Extractor

Runs regex extraction first, then T5 extraction on failures.
"""

from main_regex_extractor import process_all_records as process_regex
from main_t5_extractor import process_t5_records


def main():
    """Run full extraction pipeline: regex first, then T5 on failures."""
    print("=" * 60)
    print("Phase 1: Regex Extraction")
    print("=" * 60)
    # process_regex()

    print("\n")
    print("=" * 60)
    print("Phase 2: T5 Extraction for Regex Failures")
    print("=" * 60)
    process_t5_records()


if __name__ == "__main__":
    main()
