# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data migration and pipeline development project for the Rubicon system, focusing on Azure-based data infrastructure. The project involves:
- PostgreSQL database replication and migration
- Azure Data Factory pipeline development
- Cosmos DB integration for NoSQL storage
- Azure AI Search implementation
- Data mart design and metadata management

## Architecture

### Data Ingestion Pipeline
```
[Source PostgreSQL] → [Azure DMS] → [Target PostgreSQL (Replica)]
                                           ↓
                                   [Azure Data Factory]
                                           ↓
                                   [NoSQL (Cosmos DB)]
```

## Key Project Areas

### Database Work
- PostgreSQL replication and migration using Azure Database Migration Service
- Data profiling and schema analysis for both source and Rubicon tables
- Synonym dictionary creation
- Mock DB creation with sample data

### Data Pipeline
- Azure Data Factory configuration for ETL processes
- Cosmos DB setup for unstructured data storage
- Azure AI Search configuration for metadata and search capabilities
- Data mart design for structured storage

### Metadata Extension
- Column descriptions (short and long format)
- Table profile descriptions
- SQL-to-Text summarization

## Project Structure

- `/instruction/` - Project planning and checklist documents (Korean language)
- `/notebooks/` - Jupyter notebooks for data pipeline development

## Development Notes

- Primary documentation is in Korean - maintain language consistency when updating existing files
- The project follows Azure cloud architecture patterns
- Focus areas include data standardization, user query analysis, and metadata management