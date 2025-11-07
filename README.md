# AI_EXTRACT SiS and backend demo for extracting key/attributes from Customer Loan Documentation 
(can be leveraged for numerous unstructured extraction demo)


## Snowflake Cortex Features Demo Documentation

## Overview

This demo showcases the ai_extract function within a SiS app. Utilized to demo the feature to business with great response. Additionally, ran through the 
backend process of running it on parsed data (in the SiS app) vs directly against staged .pdf's.

1. **Initial Setup** (`Initial_Setup.sql`): Create DB/Schema/Stage to store unstructured data, runs through the ease of use of ai_extract directly against .pdf's
2. **SiS Setup** (`SiS_Setup.py`): Demonstrates AI/ML features including document processing, embeddings, search, and intelligent agents

<img width="1216" height="584" alt="Screenshot 2025-11-07 at 9 19 42 AM" src="https://github.com/user-attachments/assets/ade1516b-bebe-42d6-91f5-0b0317fdc7b4" />
<img width="1173" height="345" alt="Screenshot 2025-11-07 at 9 19 49 AM" src="https://github.com/user-attachments/assets/66697d65-8fb7-4f02-ad86-cc767ef8b158" />
<img width="307" height="663" alt="Screenshot 2025-11-07 at 9 20 13 AM" src="https://github.com/user-attachments/assets/177406d4-17d3-42d9-8c30-9d7b2e8bbb19" />

### Key Cortex Features Demonstrated

- **AI_EXTRACT (AISQL)**: Extracts information from an input string or file.
- **PARSE_DOCUMENT**: Returns the extracted content from a document on a Snowflake stage as a JSON-formatted string.


---

## Initial Setup (`Initial_Setup`)

### Purpose
1 - Sets up DB/Schema/Stage for upload of .pdf's. 
2 - Creates doc_extract table from result of AI_EXTRACT directly against pdf's

### Section 1: Database and Schema Setup 

```sql
USE ROLE accountadmin;

CREATE DATABASE IF NOT EXISTS pdf_extractor_db;
CREATE STAGE IF NOT EXISTS pdf_processing;
CREATE STAGE IF NOT EXISTS loan_docs_test;
```

**Purpose**: Establishes the foundational database structure for the demo. Once Stage is created, upload sample .pdf's that will be used for demo.

```sql
CREATE OR REPLACE TABLE pdf_extractor_db.pdf_processing.doc_extract AS
SELECT relative_path,
AI_EXTRACT(
  file => TO_FILE('@pdf_extractor_db.pdf_processing.pdf_upload_stage', relative_path),
  responseFormat => [
    'Company Name',
    'Doing Business As',
    'Primary ID Type',
    'Primary ID Number',
    'Country of Issue',
    'State/Province of Issue',
    'Date of Issue',
    'Customer Name',
    'Address Type',
    'Primary Address',
    'City',
    'County',
    'Country',
    'Postal Code',
    'State/Province',
    'Primary Contact Name',
    'Primary Contact Number',
    'Contact Type',
    'Primary Email Address',
    'Fax #',
    'Previous Address',
    'Organization ID',
    'Date Business Established',
    'Country of Incorporation',
    'State/Province of Organization',
    'Resolution Date',
    '# of Required Signers',
    '# of Employees',
    'Date Current Ownership Started'
    
  ]  
) AS extract_data
FROM DIRECTORY (@pdf_extractor_db.pdf_processing.pdf_upload_stage);
```

**Purpose**: Creates 1 row with json output from ai_extract for each corresponding pdf. [REPLACE string text with key/values/questions for extraction]

```sql
select relative_path
,extract_data:response:"Company Name"
,extract_data:response:"Doing Business As"
,extract_data:response:"Primary ID Type"
,extract_data:response:"Primary ID Number"
,extract_data:response:"State/Province of Issue"
,extract_data:response:"Date of Issue"
,extract_data:response:"Customer Name"
,extract_data:response:"Address Type"
,extract_data:response:"Primary Address"
,extract_data:response:"City"
,extract_data:response:"County"
,extract_data:response:"Country"
,extract_data:response:"Postal Code"
,extract_data:response:"Primary Contact Name"
,extract_data:response:"Primary Contact Number"
,extract_data:response:"Contact Type"
,extract_data:response:"Primary Email Address"
,extract_data:response:"Fax #"
,extract_data:response:"Previous Address"
,extract_data:response:"Date Business Established"
,extract_data:response:"Country of Incorporation"
,extract_data:response:"State/Province of Organization"
,extract_data:response:"Resolution Date"
,extract_data:response:"# of Required Signers"
,extract_data:response:"# of Employees"
,extract_data:response:"Date Current Ownership Started"
,extract_data
from pdf_extractor_db.pdf_processing.doc_extract;
```

**Purpose**: Flattens json key/pairs to columnar format for use in future pipeline work. 


### Section 2: SiS (Streamlit in Snowflake) 



**Purpose**: Creates SiS App that utilizes parse_doc and ai_extract
