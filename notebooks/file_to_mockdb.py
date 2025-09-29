#!/usr/bin/env python3
"""
PostgreSQL Mock Database Setup Module

This module handles:
1. PostgreSQL connection and table creation from CSV schema definition
2. Data upload from CSV files to PostgreSQL
3. Index creation based on index definition CSV

Usage:
    python file_to_mockdb.py --schema data.csv --index index.csv --data <data_file.csv>
"""

import os
import sys
import argparse
import pandas as pd
import json
import re
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine
import psycopg2
from psycopg2.extras import execute_batch

# Load environment variables
load_dotenv('.env')

# PostgreSQL configuration
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# SQLAlchemy connection string
POSTGRES_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"


class PostgreSQLMockDB:
    """Handles PostgreSQL mock database creation and data loading"""
    
    def __init__(self, table_name: str = 'test'):
        """Initialize PostgreSQL connection
        
        Args:
            table_name: Name of the table to create
        """
        self.table_name = table_name
        self.engine = None
        self.connection_url = POSTGRES_URL
        
    def connect(self) -> Engine:
        """Establish PostgreSQL connection
        
        Returns:
            SQLAlchemy engine object
        """
        print("=" * 60)
        print("PostgreSQL Connection")
        print("=" * 60)
        
        try:
            # Create SQLAlchemy engine
            self.engine = create_engine(self.connection_url)
            
            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version();"))
                version = result.fetchone()[0]
                print(f"✅ PostgreSQL connected successfully!")
                print(f"   Version: {version}")
                print(f"   Host: {PG_HOST}")
                print(f"   Database: {PG_DATABASE}")
                print(f"   User: {PG_USER}")
                
            return self.engine
            
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            raise
    
    def read_schema_definition(self, schema_file: str) -> pd.DataFrame:
        """Read table schema from CSV file
        
        Args:
            schema_file: Path to CSV file with columns: column_name, type, description
            
        Returns:
            DataFrame with schema definition
        """
        print(f"\n📋 Reading schema from: {schema_file}")
        
        # Read CSV with proper column names
        schema_df = pd.read_csv(schema_file, encoding='utf-8', delimiter='\t')
        print(f"   Raw columns: {list(schema_df.columns)}")
        # Standardize column names
        schema_df.columns = [col.strip().lower().replace(' ', '_') for col in schema_df.columns]
        
        # Ensure required columns exist
        required_cols = ['column_name', 'type', 'description']
        for col in required_cols:
            if col not in schema_df.columns:
                raise ValueError(f"Missing required column '{col}' in schema file")
        
        print(f"   Found {len(schema_df)} columns in schema")
        return schema_df
    
    def read_index_definition(self, index_file: str) -> List[List[str]]:
        """Read index definitions from CSV file
        
        Args:
            index_file: Path to CSV file with index column names per row
            
        Returns:
            List of index definitions (each as list of column names)
        """
        print(f"\n📍 Reading indexes from: {index_file}")
        
        indexes = []
        with open(index_file, 'r', encoding='utf-8') as f:
            for line in f:
                # Clean and split the line
                columns = [col.strip() for col in line.strip().split(',') if col.strip()]
                if columns:
                    indexes.append(columns)
        
        print(f"   Found {len(indexes)} index definitions")
        return indexes
    
    def map_data_type(self, dtype: str) -> str:
        """Map CSV data types to PostgreSQL types
        
        Args:
            dtype: Data type from CSV
            
        Returns:
            PostgreSQL data type
        """
        dtype_lower = dtype.lower().strip()
        
        # Type mapping
        type_map = {
            # String types
            'varchar': 'VARCHAR(1000)',
            'text': 'TEXT',
            'char': 'CHAR(1)',
            'string': 'VARCHAR(1000)',
            
            # Numeric types
            'int': 'INT4',
            'integer': 'INT4',
            'int4': 'INT4',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'numeric': 'NUMERIC(10,2)',
            'decimal': 'NUMERIC(10,2)',
            'float': 'FLOAT8',
            'double': 'FLOAT8',
            'real': 'FLOAT4',
            
            # Date/Time types
            'date': 'DATE',
            'timestamp': 'TIMESTAMP',
            'datetime': 'TIMESTAMP',
            'time': 'TIME',
            
            # Boolean
            'boolean': 'BOOLEAN',
            'bool': 'BOOLEAN',
            
            # JSON types
            'json': 'JSONB',
            'jsonb': 'JSONB'
        }
        
        # Check for parameterized types
        if '(' in dtype_lower:
            base_type = dtype_lower.split('(')[0]
            if base_type in ['varchar', 'char']:
                return dtype.upper()
            elif base_type in ['numeric', 'decimal']:
                return dtype.upper()
        
        # Return mapped type or original if not found
        return type_map.get(dtype_lower, dtype.upper())
    
    def create_table(self, schema_df: pd.DataFrame, drop_existing: bool = False):
        """Create PostgreSQL table based on schema definition
        
        Args:
            schema_df: DataFrame with schema definition
            drop_existing: Whether to drop existing table
        """
        print(f"\n🔨 Creating table: {self.table_name}")
        
        if not self.engine:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        with self.engine.connect() as conn:
            trans = conn.begin()
            
            try:
                # Drop existing table if requested
                if drop_existing:
                    conn.execute(text(f"DROP TABLE IF EXISTS {self.table_name} CASCADE;"))
                    print(f"   Dropped existing table {self.table_name}")
                
                # Build CREATE TABLE statement
                columns = []
                comments = []
                
                for _, row in schema_df.iterrows():
                    col_name = row['column_name'].strip()
                    col_type = self.map_data_type(row['type'])
                    col_desc = row.get('description', '')
                    
                    columns.append(f"    {col_name} {col_type}")
                    
                    if col_desc:
                        comments.append(
                            f"COMMENT ON COLUMN {self.table_name}.{col_name} "
                            f"IS '{col_desc.replace("'", "''")}';"
                        )
                
                # Create table
                create_sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} (\n"
                create_sql += ",\n".join(columns)
                create_sql += "\n);"
                
                conn.execute(text(create_sql))
                print(f"   ✅ Table {self.table_name} created")
                
                # Add table comment
                conn.execute(text(
                    f"COMMENT ON TABLE {self.table_name} IS 'Mock database table created from CSV schema';"
                ))
                
                # Add column comments
                for comment_sql in comments:
                    conn.execute(text(comment_sql))
                
                print(f"   ✅ Added {len(comments)} column comments")
                
                trans.commit()
                
            except Exception as e:
                trans.rollback()
                print(f"❌ Table creation failed: {e}")
                raise
    
    def create_indexes(self, indexes: List[List[str]]):
        """Create indexes based on index definitions
        
        Args:
            indexes: List of index definitions
        """
        print(f"\n📍 Creating indexes for {self.table_name}")
        
        if not self.engine:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        with self.engine.connect() as conn:
            trans = conn.begin()
            
            try:
                created_count = 0
                
                for idx_num, columns in enumerate(indexes, 1):
                    # Generate index name
                    if len(columns) == 1:
                        idx_name = f"idx_{self.table_name}_{columns[0]}"
                    else:
                        idx_name = f"idx_{self.table_name}_{'_'.join(columns[:2])}"
                        if len(columns) > 2:
                            idx_name += f"_{len(columns)}col"
                    
                    # Build index creation SQL
                    col_list = ', '.join(columns)
                    index_sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {self.table_name}({col_list});"
                    
                    conn.execute(text(index_sql))
                    created_count += 1
                    print(f"   ✅ Created index: {idx_name} on ({col_list})")
                
                trans.commit()
                print(f"\n   Total indexes created: {created_count}")
                
            except Exception as e:
                trans.rollback()
                print(f"❌ Index creation failed: {e}")
                raise
    
    def parse_json_field(self, value: Any) -> Optional[str]:
        """Parse JSON field for PostgreSQL JSONB column
        
        Args:
            value: Value to parse
            
        Returns:
            JSON string or None
        """
        if pd.isna(value) or value == '' or value is None:
            return None
        
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)
        
        if isinstance(value, str):
            value = value.strip()
            if not value or value in ['{}', '[]', 'null', 'NULL']:
                return None
            
            try:
                # Try to parse as JSON
                if value.startswith(('{', '[')):
                    parsed = json.loads(value)
                    return json.dumps(parsed, ensure_ascii=False)
            except json.JSONDecodeError:
                # If not valid JSON, return as-is
                return value
        
        return None
    
    def prepare_data_for_insert(self, df: pd.DataFrame, schema_df: pd.DataFrame) -> List[tuple]:
        """Prepare DataFrame data for PostgreSQL insertion
        
        Args:
            df: Data DataFrame
            schema_df: Schema definition DataFrame
            
        Returns:
            List of tuples for insertion
        """
        print(f"\n📦 Preparing {len(df)} records for insertion")
        
        prepared_data = []
        column_types = {}
        
        # Build column type mapping
        for _, row in schema_df.iterrows():
            col_name = row['column_name'].strip()
            col_type = row['type'].lower()
            column_types[col_name] = col_type
        
        # Process each data row
        for idx, row in df.iterrows():
            record = []
            
            for _, schema_row in schema_df.iterrows():
                col_name = schema_row['column_name'].strip()
                col_type = schema_row['type'].lower()
                
                # Get value from data
                if col_name in df.columns:
                    value = row[col_name]
                else:
                    value = None
                
                # Handle NULL values
                if pd.isna(value) or value == '':
                    record.append(None)
                # Handle JSON types
                elif 'json' in col_type:
                    record.append(self.parse_json_field(value))
                # Handle date types
                elif col_type in ['date', 'timestamp', 'datetime']:
                    if value:
                        try:
                            date_val = pd.to_datetime(value)
                            if col_type == 'date':
                                record.append(date_val.date())
                            else:
                                record.append(date_val)
                        except:
                            record.append(None)
                    else:
                        record.append(None)
                # Handle numeric types
                elif col_type in ['int', 'integer', 'bigint', 'smallint']:
                    try:
                        record.append(int(float(value)))
                    except:
                        record.append(None)
                elif col_type in ['numeric', 'decimal', 'float', 'double', 'real']:
                    try:
                        record.append(float(value))
                    except:
                        record.append(None)
                # Handle boolean
                elif col_type in ['boolean', 'bool']:
                    if str(value).upper() in ['TRUE', 'T', 'YES', 'Y', '1']:
                        record.append(True)
                    elif str(value).upper() in ['FALSE', 'F', 'NO', 'N', '0']:
                        record.append(False)
                    else:
                        record.append(None)
                # Handle string types
                else:
                    record.append(str(value) if value is not None else None)
            
            prepared_data.append(tuple(record))
        
        print(f"   ✅ Prepared {len(prepared_data)} records")
        return prepared_data
    
    def insert_data(self, data_file: str, schema_df: pd.DataFrame, batch_size: int = 1000):
        """Insert data from CSV file into PostgreSQL table
        
        Args:
            data_file: Path to data CSV file
            schema_df: Schema definition DataFrame
            batch_size: Number of records to insert per batch
        """
        print(f"\n📤 Uploading data from: {data_file}")
        
        # Read data file
        try:
            df = pd.read_csv(data_file, encoding='utf-8', delimiter='\t')
            print(f"   Read {len(df)} records from file")
        except Exception as e:
            print(f"❌ Failed to read data file: {e}")
            raise
        
        # Prepare data
        prepared_data = self.prepare_data_for_insert(df, schema_df)
        
        if not prepared_data:
            print("   No data to insert")
            return
        
        # Build INSERT query
        columns = [row['column_name'].strip() for _, row in schema_df.iterrows()]
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_query = f"""
        INSERT INTO {self.table_name} ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
        """
        
        # Direct psycopg2 connection for batch insert
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        
        try:
            cur = conn.cursor()
            
            total_records = len(prepared_data)
            inserted_count = 0
            failed_count = 0
            
            # Insert in batches
            for i in range(0, total_records, batch_size):
                batch = prepared_data[i:i + batch_size]
                try:
                    execute_batch(cur, insert_query, batch, page_size=batch_size)
                    conn.commit()
                    inserted_count += len(batch)
                    
                    progress = min(i + batch_size, total_records)
                    print(f"   Progress: {progress}/{total_records} ({progress*100//total_records}%)")
                    
                except Exception as batch_error:
                    print(f"   ⚠️  Batch insertion failed: {batch_error}")
                    conn.rollback()
                    
                    # Try individual inserts
                    for record in batch:
                        try:
                            cur.execute(insert_query, record)
                            conn.commit()
                            inserted_count += 1
                        except Exception as record_error:
                            failed_count += 1
                            if failed_count <= 5:
                                print(f"      Failed record: {record_error}")
                            conn.rollback()
            
            print(f"\n✅ Data upload complete:")
            print(f"   Successfully inserted: {inserted_count} records")
            print(f"   Failed: {failed_count} records")
            
        except Exception as e:
            print(f"❌ Data insertion failed: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def verify_data(self):
        """Verify inserted data"""
        print(f"\n🔍 Verifying data in {self.table_name}")
        
        if not self.engine:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        with self.engine.connect() as conn:
            # Count records
            result = conn.execute(text(f"SELECT COUNT(*) FROM {self.table_name};"))
            count = result.fetchone()[0]
            print(f"   Total records: {count}")
            
            # Sample data
            result = conn.execute(text(f"SELECT * FROM {self.table_name} LIMIT 3;"))
            samples = result.fetchall()
            
            if samples:
                print("\n   Sample records:")
                for i, sample in enumerate(samples, 1):
                    print(f"   Record {i}: {sample[:5]}...")  # Show first 5 columns


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='PostgreSQL Mock Database Setup from CSV Schema'
    )
    parser.add_argument(
        '--schema', 
        required=True,
        help='Path to schema CSV file (columns: column_name, type, description)'
    )
    parser.add_argument(
        '--index',
        help='Path to index CSV file (each row contains column names for an index)'
    )
    parser.add_argument(
        '--data',
        help='Path to data CSV file to upload'
    )
    parser.add_argument(
        '--table',
        default='test',
        help='Table name to create (default: test)'
    )
    parser.add_argument(
        '--drop',
        action='store_true',
        help='Drop existing table before creating'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for data insertion (default: 1000)'
    )
    
    args = parser.parse_args()
    
    # Initialize PostgreSQL handler
    pg_handler = PostgreSQLMockDB(table_name=args.table)
    
    try:
        # Connect to database
        pg_handler.connect()
        
        # Read schema definition
        schema_df = pg_handler.read_schema_definition(args.schema)
        
        # Create table
        print(f"\n🚀 Starting mock database setup...")
        pg_handler.create_table(schema_df, drop_existing=args.drop)
        
        # Create indexes if provided
        if args.index:
            indexes = pg_handler.read_index_definition(args.index)
            if indexes:
                pg_handler.create_indexes(indexes)
        
        # Upload data if provided
        if args.data:
            pg_handler.insert_data(args.data, schema_df, batch_size=args.batch_size)
            
            # Verify data
            pg_handler.verify_data()
        
        print("\n✅ Mock database setup completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()