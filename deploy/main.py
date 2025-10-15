#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main execution script
PostgreSQL to MongoDB data ingestion pipeline:
1. Direct PostgreSQL to MongoDB data transfer
2. Metadata generation via Azure OpenAI and MongoDB storage
"""

import os
import sys
import time
import argparse
from datetime import datetime, date, time as dt_time
from typing import List, Optional
import pandas as pd
import numpy as np

# Module imports
from db_connector import DatabaseConnector
from schema_analyzer import SchemaAnalyzer
from openai_generator import OpenAIMetadataGenerator
from mongodb_saver import MongoDBSaver


# Default selected columns list
DEFAULT_SELECTED_COLUMNS = [
    'display_category_major',
    'display_category_middle',
    'display_category_minor',
    'product_category_major',
    'product_category_middle',
    'product_category_minor',
    'model_name',
    'model_code',
    'product_id',
    'product_name',
    'product_color',
    'release_date',
    'is_ai_subscription_eligible',
    'is_smart_subscription_eligible',
    'is_galaxy_club_eligible',
    'is_installment_payment_available',
    'product_detail_url',
    'unique_selling_point',
    'review_count',
    'review_rating_score',
    'standard_price',
    'member_price',
    'benefit_price',
    'review_text_collection',
    'product_specification',
    'web_coupon_discount_amount',
    'stock_quantity',
    'bundle_component_model_code',
    'site_code',
    'final_price',
    'is_bispokle_goods',
    'is_bundle_product',
    'category_rank_recommend',
    'category_rank_quantity',
    'category_rank_rating',
    'total_sale_amount',
    'total_sale_quantity',
    'event_info',
    'coupon_info',
    'promotion_info'
]


class DataIngestionPipeline:
    """Data ingestion pipeline main class"""

    def __init__(self, table_name: str,
                 direct_collection_name: str,
                 metadata_collection_name: str,
                 selected_columns: List[str] = None,
                 sample_size: int = 10000):
        self.table_name = table_name
        self.direct_collection_name = direct_collection_name
        self.metadata_collection_name = metadata_collection_name
        self.selected_columns = selected_columns or DEFAULT_SELECTED_COLUMNS
        self.sample_size = sample_size

        # Initialize components
        self.schema_analyzer = SchemaAnalyzer(table_name)
        self.openai_generator = OpenAIMetadataGenerator()
        self.mongodb_saver = MongoDBSaver()

    def run_direct_transfer(self, batch_size: int = 50) -> bool:
        """Direct data transfer from PostgreSQL to MongoDB"""
        print("\n" + "=" * 60)
        print("PostgreSQL to MongoDB Direct Data Transfer")
        print("=" * 60)

        try:
            # 1. Get total row count
            print("\n1. Getting table row count...")
            total_rows = self.schema_analyzer.get_table_row_count()
            if total_rows is None:
                print("[ERROR] Failed to get row count")
                return False

            if total_rows == 0:
                print(f"[WARNING] Table '{self.table_name}' is empty")
                return False

            print(f"   Total rows in table: {total_rows:,}")

            # 2. Get MongoDB collection
            print("\n2. Connecting to MongoDB...")
            client = self.mongodb_saver.db_connector.get_mongodb_client()
            if not client:
                print("[ERROR] MongoDB connection failed")
                return False

            db = self.mongodb_saver.db_connector.get_mongodb_database(
                client, self.mongodb_saver.db_name
            )
            collection = db[self.direct_collection_name]
            print(f"   Database: {self.mongodb_saver.db_name}")
            print(f"   Collection: {self.direct_collection_name}")

            # 3. Delete existing data (optional)
            try:
                delete_result = collection.delete_many({"imported_from": "PostgreSQL"})
                if delete_result.deleted_count > 0:
                    print(f"   Deleted {delete_result.deleted_count} existing PostgreSQL records")
            except Exception as e:
                print(f"   Warning: Failed to delete existing data: {e}")

            # 4. Transfer data in batches
            print(f"\n3. Starting data transfer (batch size: {batch_size})...")
            inserted_count = 0
            failed_count = 0
            failed_samples = []

            # Process data in chunks to avoid memory issues
            chunk_size = 1000  # Read 1000 rows at a time from PostgreSQL

            for offset in range(0, total_rows, chunk_size):
                # Get chunk of data from PostgreSQL
                df_chunk = self.schema_analyzer.get_table_data(
                    limit=chunk_size,
                    offset=offset
                )

                if df_chunk is None or df_chunk.empty:
                    continue

                # Convert DataFrame to list of documents
                documents = df_chunk.to_dict('records')

                # Add metadata to each document
                for doc in documents:
                    # Clean document for MongoDB compatibility
                    cleaned_doc = {
                        'imported_from': 'PostgreSQL',
                        'imported_at': datetime.now()
                    }

                    for key, value in doc.items():
                        # Skip MongoDB internal fields that might exist
                        if key in ['_id', 'imported_from', 'imported_at']:
                            continue
                        # Skip keys that start with underscore (MongoDB reserved)
                        if key.startswith('_'):
                            continue

                        # Handle different data types
                        if value is None:
                            cleaned_doc[key] = None
                        elif pd.api.types.is_scalar(value):
                            # Check for NaN/NaT values
                            if pd.isna(value):
                                cleaned_doc[key] = None
                            # Convert numpy types to Python types
                            elif isinstance(value, (np.integer, np.int64)):
                                cleaned_doc[key] = int(value)
                            elif isinstance(value, (np.floating, np.float64)):
                                if np.isnan(value) or np.isinf(value):
                                    cleaned_doc[key] = None
                                else:
                                    cleaned_doc[key] = float(value)
                            elif isinstance(value, np.bool_):
                                cleaned_doc[key] = bool(value)
                            elif isinstance(value, pd.Timestamp):
                                cleaned_doc[key] = value.to_pydatetime()
                            elif isinstance(value, datetime):
                                # datetime.datetime is OK for MongoDB
                                cleaned_doc[key] = value
                            elif isinstance(value, date):
                                # Convert datetime.date to datetime.datetime
                                cleaned_doc[key] = datetime.combine(value, dt_time.min)
                            else:
                                cleaned_doc[key] = value
                        elif isinstance(value, np.ndarray):
                            # Convert numpy arrays to lists
                            cleaned_doc[key] = value.tolist()
                        elif isinstance(value, pd.Series):
                            # Convert pandas Series to list
                            cleaned_doc[key] = value.tolist()
                        elif isinstance(value, (list, dict)):
                            # Keep lists and dicts as is
                            cleaned_doc[key] = value
                        else:
                            # Convert other types to string
                            cleaned_doc[key] = str(value)

                    # Update the document with cleaned values
                    doc.clear()
                    doc.update(cleaned_doc)

                # Insert in smaller batches
                for i in range(0, len(documents), batch_size):
                    batch = documents[i:i + batch_size]

                    try:
                        result = collection.insert_many(batch, ordered=False)
                        inserted_count += len(result.inserted_ids)
                    except Exception as batch_error:
                        error_msg = str(batch_error)

                        # Try individual inserts for failed batch
                        if "duplicate key" in error_msg.lower():
                            print(f"   Warning: Duplicate keys detected - trying individual inserts")
                        else:
                            # Print more detailed error information
                            if "Invalid document" in error_msg:
                                print(f"   Warning: Invalid document format detected")
                            else:
                                print(f"   Warning: Batch insert failed: {error_msg[:200]}")

                        for doc in batch:
                            try:
                                collection.insert_one(doc)
                                inserted_count += 1
                            except Exception as doc_error:
                                failed_count += 1
                                error_str = str(doc_error)

                                if failed_count <= 5:  # Save first 5 errors as samples
                                    # Try to identify the problematic field
                                    problem_fields = []
                                    for k, v in doc.items():
                                        if v is not None and not isinstance(v, (str, int, float, bool, list, dict, datetime)):
                                            problem_fields.append(f"{k}:{type(v).__name__}")

                                    failed_samples.append({
                                        'id': doc.get('product_id', doc.get('model_code', 'Unknown')),
                                        'error': error_str[:150],
                                        'problem_fields': problem_fields if problem_fields else None
                                    })

                # Progress update
                progress = min(offset + chunk_size, total_rows)
                percent = (progress * 100) // total_rows
                print(f"   Progress: {progress:,}/{total_rows:,} ({percent}%)")

            # 5. Print results
            print(f"\n4. Data transfer completed:")
            print(f"   Successfully inserted: {inserted_count:,} documents")
            print(f"   Failed: {failed_count} documents")

            if failed_samples:
                print(f"\n   Failed samples:")
                for sample in failed_samples:
                    print(f"      - {sample['id']}: {sample['error']}")
                    if sample.get('problem_fields'):
                        print(f"        Problem fields: {', '.join(sample['problem_fields'])}")

            # 6. Collection statistics
            print("\n5. Collection statistics:")
            try:
                total_docs = collection.count_documents({})
                pg_docs = collection.count_documents({"imported_from": "PostgreSQL"})
                print(f"   Total documents: {total_docs:,}")
                print(f"   PostgreSQL documents: {pg_docs:,}")

                # Sample document
                sample = collection.find_one({"imported_from": "PostgreSQL"})
                if sample:
                    print("\n   Sample document:")
                    # Remove MongoDB internal fields for display
                    sample_display = {k: v for k, v in sample.items() if k not in ['_id', 'imported_at']}

                    # Show first few fields
                    for i, (key, value) in enumerate(list(sample_display.items())[:5]):
                        if value is not None:
                            if isinstance(value, (int, float)):
                                print(f"      {key}: {value:,}" if isinstance(value, int) else f"      {key}: {value}")
                            else:
                                value_str = str(value)[:50] + '...' if len(str(value)) > 50 else str(value)
                                print(f"      {key}: {value_str}")
            except Exception as stat_error:
                print(f"   Warning: Failed to get statistics: {stat_error}")

            return inserted_count > 0

        except Exception as e:
            print(f"[ERROR] Error during direct transfer: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            if 'client' in locals():
                client.close()
                print("\n   MongoDB connection closed")

    def run_with_metadata_generation(self, batch_size: int = 5) -> bool:
        """Generate metadata via Azure OpenAI and save to MongoDB"""
        print("\n" + "=" * 60)
        print("Azure OpenAI Metadata Generation and MongoDB Storage")
        print("=" * 60)

        try:
            # 1. Get table schema
            print("\n1. Fetching table schema...")
            df_schema = self.schema_analyzer.get_table_schema()
            if df_schema is None:
                print("[ERROR] Schema fetch failed")
                return False

            # 2. Collect column statistics
            print("\n2. Collecting column statistics...")
            df_column_stats = self.schema_analyzer.get_column_statistics(self.sample_size)
            if df_column_stats is None:
                print("[ERROR] Column statistics collection failed")
                return False

            # 3. Generate metadata for selected columns
            print(f"\n3. Generating metadata for {len(self.selected_columns)} columns...")
            df_metadata = self._generate_metadata_for_columns(
                df_column_stats, df_schema, batch_size
            )

            # 4. Save to MongoDB
            print("\n4. Saving metadata to MongoDB...")
            success = self.mongodb_saver.save_metadata_to_collection(
                df_column_stats=df_column_stats,
                df_metadata=df_metadata,
                df_schema=df_schema,
                table_name=self.table_name,
                selected_columns=self.selected_columns,
                collection_name=self.metadata_collection_name
            )

            if success:
                print("[SUCCESS] Metadata generation and storage completed")
            else:
                print("[ERROR] Metadata storage failed")

            return success

        except Exception as e:
            print(f"[ERROR] Error during metadata generation: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _generate_metadata_for_columns(
        self,
        df_column_stats: pd.DataFrame,
        df_schema: pd.DataFrame,
        batch_size: int
    ) -> pd.DataFrame:
        """Generate metadata for selected columns"""
        metadata_list = []

        # Filter selected columns only
        selected_stats = df_column_stats[
            df_column_stats['column_name'].isin(self.selected_columns)
        ]

        total_columns = len(selected_stats)
        print(f"Processing {total_columns} columns (batch size: {batch_size})")

        for i in range(0, total_columns, batch_size):
            batch_end = min(i + batch_size, total_columns)
            print(f"\nBatch processing: {i+1}-{batch_end}/{total_columns}")

            for idx in range(i, batch_end):
                column_info = selected_stats.iloc[idx].to_dict()
                column_name = column_info['column_name']

                # Add schema info
                if df_schema is not None:
                    schema_row = df_schema[df_schema['column_name'] == column_name]
                    if not schema_row.empty:
                        column_info['column_comment'] = schema_row.iloc[0]['column_comment']

                print(f"  - Processing {column_name}...")

                # Generate OpenAI description
                description = self.openai_generator.generate_column_description(
                    column_info,
                    self.table_name
                )

                # Build metadata
                metadata = {
                    'column_name': column_name,
                    'data_type': column_info.get('data_type'),
                    'null_ratio': column_info.get('null_ratio'),
                    'unique_count': column_info.get('unique_count'),
                    'description': description,
                    'generated_at': datetime.now().isoformat()
                }

                # Add numeric data info if available
                if 'min' in column_info and pd.notna(column_info['min']):
                    metadata.update({
                        'min': column_info.get('min'),
                        'max': column_info.get('max'),
                        'mean': column_info.get('mean'),
                        'median': column_info.get('median')
                    })

                metadata_list.append(metadata)

                # Rate limiting for API calls
                time.sleep(0.5)

        print(f"\n[SUCCESS] Generated metadata for {len(metadata_list)} columns")
        return pd.DataFrame(metadata_list)

    def generate_table_description(self) -> Optional[str]:
        """Generate overall table description"""
        print("\nGenerating table description...")

        # Get schema info
        df_schema = self.schema_analyzer.get_table_schema()
        df_column_stats = self.schema_analyzer.get_column_statistics(1000)

        if df_column_stats is None:
            print("[ERROR] Cannot retrieve table statistics")
            return None

        # Table statistics (simplified version)
        table_stats = {
            'total_rows': len(df_column_stats),
            'table_size': 'N/A'
        }

        description = self.openai_generator.generate_table_description(
            self.table_name,
            table_stats,
            df_column_stats
        )

        if description:
            print("[SUCCESS] Table description generated")
            print("-" * 50)
            print(description)
            print("-" * 50)
        else:
            print("[ERROR] Table description generation failed")

        return description


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='PostgreSQL to MongoDB Data Ingestion Pipeline'
    )
    parser.add_argument(
        '--table',
        type=str,
        default='kt_merged_product_20251001',
        help='PostgreSQL table name'
    )
    parser.add_argument(
        '--direct-collection',
        type=str,
        default='_product_data_direct',
        help='MongoDB collection name for direct PostgreSQL data transfer'
    )
    parser.add_argument(
        '--metadata-collection',
        type=str,
        default='_synonyms_20251014',
        help='MongoDB collection name for metadata with OpenAI descriptions'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['direct', 'metadata', 'both'],
        default='both',
        help='Execution mode: direct, metadata, or both'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=10000,
        help='Sample size for statistics analysis'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=5,
        help='OpenAI API batch size'
    )
    parser.add_argument(
        '--generate-table-desc',
        action='store_true',
        help='Generate table description'
    )

    args = parser.parse_args()

    # Initialize pipeline
    pipeline = DataIngestionPipeline(
        table_name=args.table,
        direct_collection_name=args.direct_collection,
        metadata_collection_name=args.metadata_collection,
        selected_columns=DEFAULT_SELECTED_COLUMNS,
        sample_size=args.sample_size
    )

    print("\n" + "=" * 60)
    print("Data Ingestion Pipeline Started")
    print("=" * 60)
    print(f"Table: {args.table}")
    print(f"Direct Collection: {args.direct_collection}")
    print(f"Metadata Collection: {args.metadata_collection}")
    print(f"Mode: {args.mode}")
    print(f"Sample size: {args.sample_size}")

    success = True

    # Execute based on mode
    if args.mode in ['direct', 'both']:
        success = success and pipeline.run_direct_transfer(batch_size=args.batch_size)

    if args.mode in ['metadata', 'both']:
        success = success and pipeline.run_with_metadata_generation(batch_size=args.batch_size)

    # Generate table description (optional)
    if args.generate_table_desc:
        pipeline.generate_table_description()

    # Final result
    print("\n" + "=" * 60)
    if success:
        print("Pipeline execution completed successfully")
    else:
        print("Pipeline execution partially failed")
    print("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())