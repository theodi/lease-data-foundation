"""
Safely delete short-term leases (tenure years below a threshold) from MongoDB.

The ``leasesext`` collection holds the parsed ``ty`` (tenure years) field. Each
``leasesext`` document references its parent ``leases`` document via the ``lid``
field (``leasesext.lid == leases._id``), a 1-to-1 relationship.

Because the dataset is large (~7M documents, ~270K deletions expected) and the
deletion must be crash-safe, this script is split into two phases:

1. ``collect`` — Scan ``leasesext`` for ``ty < threshold`` and persist every
   target id pair (``leasesext._id`` and the related ``leases._id``) to a file
   on disk. This is a read-only phase.

2. ``delete`` — Stream the id file and delete the documents from BOTH
   collections in batches. A progress file records how many id pairs have been
   fully processed, so the phase can be safely re-run/resumed after any error
   and will continue exactly where it left off until both pairs of every record
   are deleted.

Both phases default to a DRY-RUN. Pass ``--apply`` to make real changes.

Examples
--------
# 1) Collect target ids to the id file (read-only)
python -m src.db_patch.delete_short_leases collect

# 2) Dry-run the deletion (no changes)
python -m src.db_patch.delete_short_leases delete

# 3) Actually delete from both collections
python -m src.db_patch.delete_short_leases delete --apply

# Or run both phases in one go
python -m src.db_patch.delete_short_leases all --apply
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from bson import ObjectId
from dotenv import load_dotenv
from pymongo import DeleteOne
from pymongo.errors import BulkWriteError
from tqdm import tqdm

from src.utils.mongo_client import MongoDBClient

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Default tenure-year threshold: leases with ty strictly below this are deleted.
DEFAULT_MIN_TENURE_YEARS = 21

# Batch sizes
COLLECT_BATCH_SIZE = 50_000
DELETE_BATCH_SIZE = 1_000

# Files live alongside this script in the src/db_patch folder.
PATCH_DIR = Path(__file__).resolve().parent
ID_FILE = PATCH_DIR / "short_lease_ids.jsonl"
PROGRESS_FILE = PATCH_DIR / "short_lease_ids.progress"


def collect_target_ids(
    leasesext_collection,
    min_tenure_years: int,
    id_file: Path,
) -> int:
    """
    Phase 1: find all leasesext docs with ty < threshold and write the id pairs
    to ``id_file`` as JSON lines.

    Each line has the form: {"ext_id": "<hex>", "lease_id": "<hex>"}

    This phase is read-only against MongoDB.

    Args:
        leasesext_collection: MongoDB leasesext collection
        min_tenure_years: Tenure-year threshold (delete ty < this value)
        id_file: Path where the id pairs are written

    Returns:
        Number of id pairs written
    """
    query = {"ty": {"$lt": min_tenure_years}}
    projection = {"_id": 1, "lid": 1}

    logger.info(f"🔎 Counting leasesext documents with ty < {min_tenure_years}...")
    total = leasesext_collection.count_documents(query)
    logger.info(f"📊 {total} short-term leasesext documents found")

    cursor = leasesext_collection.find(query, projection).batch_size(COLLECT_BATCH_SIZE)

    written = 0
    missing_lid = 0

    # Write atomically to a temp file, then move into place on success.
    tmp_file = id_file.with_suffix(id_file.suffix + ".tmp")
    with open(tmp_file, "w", encoding="utf-8") as f:
        for doc in tqdm(cursor, total=total, desc="Collecting ids", unit="docs"):
            ext_id = doc.get("_id")
            lease_id = doc.get("lid")
            if lease_id is None:
                # No parent reference; we can still delete the ext doc, but log it.
                missing_lid += 1
            f.write(json.dumps({
                "ext_id": str(ext_id),
                "lease_id": str(lease_id) if lease_id is not None else None,
            }) + "\n")
            written += 1

    tmp_file.replace(id_file)

    # Reset any stale progress from a previous run since the id file is new.
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()

    logger.info(f"✅ Wrote {written} id pairs to {id_file}")
    if missing_lid:
        logger.warning(
            f"⚠️ {missing_lid} leasesext docs had no 'lid' (no related leases doc to delete)"
        )
    return written


def _read_progress() -> int:
    """Return the number of id pairs already fully processed."""
    if PROGRESS_FILE.exists():
        try:
            return int(PROGRESS_FILE.read_text().strip() or "0")
        except ValueError:
            return 0
    return 0


def _write_progress(count: int) -> None:
    """Persist the number of id pairs fully processed (flushed to disk)."""
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        f.write(str(count))
        f.flush()
        os.fsync(f.fileno())


def _count_lines(path: Path) -> int:
    """Count lines in a file efficiently."""
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)


def _delete_batch(
    leases_collection,
    leasesext_collection,
    ext_ids: List[ObjectId],
    lease_ids: List[ObjectId],
) -> Tuple[int, int]:
    """
    Delete one batch from both collections.

    Deletes the leasesext docs first, then the related leases docs, so that we
    never leave an orphaned leasesext pointing at an already-deleted lease.

    Returns:
        Tuple of (leasesext_deleted, leases_deleted)
    """
    ext_deleted = 0
    lease_deleted = 0

    if ext_ids:
        try:
            res = leasesext_collection.bulk_write(
                [DeleteOne({"_id": _id}) for _id in ext_ids], ordered=False
            )
            ext_deleted = res.deleted_count
        except BulkWriteError as e:
            logger.warning(f"⚠️ leasesext bulk delete error: {e.details}")
            ext_deleted = e.details.get("nRemoved", 0)

    if lease_ids:
        try:
            res = leases_collection.bulk_write(
                [DeleteOne({"_id": _id}) for _id in lease_ids], ordered=False
            )
            lease_deleted = res.deleted_count
        except BulkWriteError as e:
            logger.warning(f"⚠️ leases bulk delete error: {e.details}")
            lease_deleted = e.details.get("nRemoved", 0)

    return ext_deleted, lease_deleted


def delete_target_ids(
    leases_collection,
    leasesext_collection,
    id_file: Path,
    dry_run: bool,
) -> Dict[str, int]:
    """
    Phase 2: stream the id file and delete the documents from both collections.

    Progress is checkpointed after every batch so the phase is resumable: if it
    fails partway through, re-running it skips already-processed id pairs and
    continues until every pair has been deleted from both collections.

    Args:
        leases_collection: MongoDB leases collection
        leasesext_collection: MongoDB leasesext collection
        id_file: Path to the JSONL id file produced by the collect phase
        dry_run: When True, no documents are deleted

    Returns:
        Dictionary with deletion counts
    """
    if not id_file.exists():
        raise FileNotFoundError(
            f"Id file not found: {id_file}. Run the 'collect' phase first."
        )

    total = _count_lines(id_file)
    already_done = _read_progress() if not dry_run else 0

    if already_done >= total and total > 0:
        logger.info(f"✅ Nothing to do — all {total} id pairs already processed.")
        return {"ext_deleted": 0, "leases_deleted": 0, "processed": already_done}

    if already_done:
        logger.info(f"↩️ Resuming from checkpoint: {already_done}/{total} already processed")

    mode = "DRY-RUN" if dry_run else "APPLY"
    logger.info(f"🗑️ Deleting {total - already_done} remaining id pairs ({mode})...")

    ext_deleted_total = 0
    leases_deleted_total = 0
    processed = already_done

    ext_batch: List[ObjectId] = []
    lease_batch: List[ObjectId] = []

    with open(id_file, "r", encoding="utf-8") as f:
        # Skip already-processed lines.
        for _ in range(already_done):
            f.readline()

        pbar = tqdm(total=total, initial=already_done, desc="Deleting pairs", unit="pairs")
        for line in f:
            line = line.strip()
            if not line:
                processed += 1
                pbar.update(1)
                continue

            entry = json.loads(line)
            ext_batch.append(ObjectId(entry["ext_id"]))
            if entry.get("lease_id"):
                lease_batch.append(ObjectId(entry["lease_id"]))

            if len(ext_batch) >= DELETE_BATCH_SIZE:
                if not dry_run:
                    ed, ld = _delete_batch(
                        leases_collection, leasesext_collection, ext_batch, lease_batch
                    )
                    ext_deleted_total += ed
                    leases_deleted_total += ld

                processed += len(ext_batch)
                pbar.update(len(ext_batch))

                if not dry_run:
                    _write_progress(processed)

                ext_batch = []
                lease_batch = []

        # Flush the final partial batch.
        if ext_batch:
            if not dry_run:
                ed, ld = _delete_batch(
                    leases_collection, leasesext_collection, ext_batch, lease_batch
                )
                ext_deleted_total += ed
                leases_deleted_total += ld

            processed += len(ext_batch)
            pbar.update(len(ext_batch))

            if not dry_run:
                _write_progress(processed)

        pbar.close()

    if dry_run:
        logger.info(
            f"🧪 DRY-RUN complete — would delete up to {total - already_done} "
            f"leasesext docs and their related leases docs."
        )
    else:
        logger.info(
            f"✅ Deletion complete — removed {ext_deleted_total} leasesext docs "
            f"and {leases_deleted_total} leases docs."
        )

    return {
        "ext_deleted": ext_deleted_total,
        "leases_deleted": leases_deleted_total,
        "processed": processed,
    }


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Safely delete short-term leases (ty < threshold) from MongoDB."
    )
    parser.add_argument(
        "phase",
        choices=["collect", "delete", "all"],
        help="Which phase to run: 'collect' ids, 'delete' them, or 'all'.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply deletions to the database (default is dry-run).",
    )
    parser.add_argument(
        "--min-tenure-years",
        type=int,
        default=DEFAULT_MIN_TENURE_YEARS,
        help=f"Delete leases with ty below this value (default: {DEFAULT_MIN_TENURE_YEARS}).",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=os.getenv("MONGO_DATABASE"),
        help="MongoDB database name (default from env file).",
    )
    parser.add_argument(
        "--collection",
        type=str,
        default=os.getenv("MONGO_COLLECTION", "leases"),
        help="MongoDB leases collection name (default from env file).",
    )
    parser.add_argument(
        "--collection-ext",
        type=str,
        default=os.getenv("MONGO_COLLECTION_EXT", "leasesext"),
        help="MongoDB leasesext collection name (default from env file).",
    )
    parser.add_argument(
        "--connection-string",
        type=str,
        default=os.getenv("MONGO_URI"),
        help="MongoDB connection string (default from env file).",
    )
    parser.add_argument(
        "--id-file",
        type=str,
        default=str(ID_FILE),
        help=f"Path to the id file (default: {ID_FILE}).",
    )

    args = parser.parse_args()

    dry_run = not args.apply
    id_file = Path(args.id_file)

    if dry_run:
        logger.info("🧪 Running in DRY-RUN mode — no database changes will be made.")
    else:
        logger.info("🚨 APPLY mode — database will be modified.")

    client = MongoDBClient(
        connection_string=args.connection_string,
        database_name=args.database,
    )

    try:
        with client:
            leases_collection = client.get_collection(args.collection)
            leasesext_collection = client.get_collection(args.collection_ext)
            logger.info(
                f"📦 Connected to {args.database}: "
                f"{args.collection} / {args.collection_ext}"
            )

            if args.phase in ("collect", "all"):
                # Collection is read-only, so it always runs regardless of dry-run.
                collect_target_ids(
                    leasesext_collection, args.min_tenure_years, id_file
                )

            if args.phase in ("delete", "all"):
                result = delete_target_ids(
                    leases_collection, leasesext_collection, id_file, dry_run
                )
                logger.info(f"📊 Result: {result}")

    except Exception as e:
        import traceback
        logger.error(f"❌ Error:\n{traceback.format_exc()}")
        logger.error(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


