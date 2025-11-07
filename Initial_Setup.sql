USE ROLE accountadmin;

CREATE DATABASE IF NOT EXISTS pdf_extractor_db;
CREATE STAGE IF NOT EXISTS pdf_processing;
CREATE STAGE IF NOT EXISTS loan_docs_test;

ls @fis_acbs.data.loan_docs;


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

select * from pdf_extractor_db.pdf_processing.doc_extract;

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
