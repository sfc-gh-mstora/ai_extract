"""
Streamlit in Snowflake App: PDF Data Extractor (Stage-Based)
Compatible with Streamlit 1.22.0+
Users upload PDFs to stage first, then select them in the app for extraction.
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import json
import io

# Get the active Snowflake session
session = get_active_session()

# App configuration
st.set_page_config(
    page_title="PDF Data Extractor",
    page_icon="📄",
    layout="wide"
)

# Title and description
st.title("📄 PDF Data Extractor with AI_EXTRACT")
st.markdown("""
Extract structured data from PDF documents using Snowflake's CORTEX AI_EXTRACT.
Upload PDFs to the stage, define extraction fields, and download results as CSV.
""")

# Initialize session state
if 'extracted_data' not in st.session_state:
    st.session_state.extracted_data = None
if 'extraction_fields' not in st.session_state:
    st.session_state.extraction_fields = []
if 'available_files' not in st.session_state:
    st.session_state.available_files = []
if 'selected_file' not in st.session_state:
    st.session_state.selected_file = None

# Sidebar for configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Stage selection
    st.subheader("Snowflake Stage")
    stage_name = st.text_input(
        "Stage Name",
        value="pdf_upload_stage",
        help="The Snowflake stage where PDFs are stored (without @)"
    )
    
    # Refresh files button
    if st.button("🔄 Refresh File List"):
        st.session_state.available_files = []
        st.experimental_rerun()
    
    st.markdown("---")
    
    # Field definition
    st.subheader("📋 Define Extraction Fields")
    st.markdown("Add fields you want to extract from the PDF:")
    
    # Add new field
    new_field = st.text_input("Field Name", key="new_field")
    field_description = st.text_area(
        "Field Description (optional)",
        help="Provide context to help the AI understand what to extract",
        key="field_desc"
    )
    
    if st.button("➕ Add Field"):
        if new_field:
            st.session_state.extraction_fields.append({
                "name": new_field,
                "description": field_description
            })
            st.success(f"Added field: {new_field}")
            st.experimental_rerun()
    
    # Display current fields
    if st.session_state.extraction_fields:
        st.markdown("**Current Fields:**")
        for idx, field in enumerate(st.session_state.extraction_fields):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.text(f"• {field['name']}")
                if field['description']:
                    st.caption(field['description'])
            with col2:
                if st.button("🗑️", key=f"delete_{idx}"):
                    st.session_state.extraction_fields.pop(idx)
                    st.experimental_rerun()
    
    # Quick templates
    st.markdown("---")
    st.subheader("🚀 Quick Templates")
    
    if st.button("Loan Docs: Address"):
        st.session_state.extraction_fields = [
            {"name": "Address Type", "description": "The classification of the address"},
            {"name": "Primary Address", "description": "The street address of the customers primary location"},
            {"name": "City", "description": "The city of the primary address"},
            {"name": "State/Province", "description": "The state or province for the primary or previous address"},
            {"name": "Postal Code", "description": "The zip or postal code for the primary or previous address"},
            {"name": "Country", "description": "The country of the primary or previous address"},
            {"name": "Primary Contact Number", "description": "The telephone number of the main point of contact"}
        ]
        st.experimental_rerun()
    
    if st.button("Loan Docs: Initial"):
        st.session_state.extraction_fields = [
            {"name": "Company Name", "description": "The official legal name of the entity"},
            {"name": "Data of Issue", "description": "What is the date the primary identification was issued"},
            {"name": "Default Currency", "description": "What is the monetary unit used for financial transactions and reporting for the group (eg, US Dollar)"},
            {"name": "Primary ID Number", "description": "The specific unique identification number"},
            {"name": "# of Employees", "description": "The total headcount of the business"}
        ]
        st.experimental_rerun()
    
    
    
    if st.button("Contract Template"):
        st.session_state.extraction_fields = [
            {"name": "contract_number", "description": "Contract ID or number"},
            {"name": "effective_date", "description": "Contract effective date"},
            {"name": "expiration_date", "description": "Contract expiration date"},
            {"name": "parties", "description": "Names of parties involved"},
            {"name": "contract_value", "description": "Total contract value"},
            {"name": "terms", "description": "Key terms and conditions"}
        ]
        st.experimental_rerun()
    
    if st.button("Clear All Fields"):
        st.session_state.extraction_fields = []
        st.experimental_rerun()

# Main content area
st.header("📁 Step 1: Upload PDF to Stage")

st.info("""
**To upload PDFs to the stage, use one of these methods:**

**Option A: Via Snowsight UI**
1. Navigate to: Data → PDF_EXTRACTOR_DB → PDF_PROCESSING → Stages → {stage_name}
2. Click "Upload Files"
3. Select your PDF(s)

**Option B: Via SQL**
```sql
PUT file:///path/to/your/file.pdf @{stage_name} AUTO_COMPRESS=FALSE;
```

After uploading, click "🔄 Refresh File List" in the sidebar.
""".format(stage_name=stage_name))

st.markdown("---")

# Step 2: List available files
st.header("📋 Step 2: Select PDF File")

# Get list of files from stage if not already loaded
if not st.session_state.available_files:
    try:
        # Create stage if it doesn't exist
        session.sql(f"CREATE STAGE IF NOT EXISTS {stage_name}").collect()
        
        # List files in stage
        list_query = f"LIST @{stage_name}"
        result = session.sql(list_query).collect()
        
        # Filter for PDF files
        pdf_files = []
        for row in result:
            file_name = row['name']
            # Extract just the filename from the full path
            if file_name.lower().endswith('.pdf'):
                # The name column typically contains: stage_name/filename
                parts = file_name.split('/')
                filename_only = parts[-1] if len(parts) > 1 else file_name
                pdf_files.append({
                    'display_name': filename_only,
                    'full_path': file_name,
                    'size': row['size']
                })
        
        st.session_state.available_files = pdf_files
        
    except Exception as e:
        st.error(f"Error listing files: {str(e)}")
        st.info(f"💡 Make sure the stage '{stage_name}' exists and contains PDF files")

# Display available files
if st.session_state.available_files:
    st.success(f"✅ Found {len(st.session_state.available_files)} PDF file(s) in stage")
    
    # Create a selectbox with file names
    file_options = [f"{f['display_name']} ({f['size']} bytes)" for f in st.session_state.available_files]
    
    selected_index = st.selectbox(
        "Select a PDF file to process:",
        range(len(file_options)),
        format_func=lambda i: file_options[i]
    )
    
    st.session_state.selected_file = st.session_state.available_files[selected_index]
    
    # Show selected file info
    with st.expander("📎 File Details"):
        st.write(f"**Name:** {st.session_state.selected_file['display_name']}")
        st.write(f"**Size:** {st.session_state.selected_file['size']} bytes")
        st.write(f"**Path:** {st.session_state.selected_file['full_path']}")
else:
    st.warning("⚠️ No PDF files found in the stage. Please upload a PDF file first.")
    st.info("After uploading, click '🔄 Refresh File List' in the sidebar.")

st.markdown("---")

# Step 3: Extract data
st.header("🚀 Step 3: Extract Data")

can_extract = (
    st.session_state.selected_file is not None 
    and len(st.session_state.extraction_fields) > 0
)

if not st.session_state.extraction_fields:
    st.warning("⚠️ Please define at least one extraction field in the sidebar.")
elif st.session_state.selected_file is None:
    st.warning("⚠️ Please select a PDF file from the list above.")
else:
    st.success("✅ Ready to extract data!")
    
    # Show what will be extracted
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Selected File", st.session_state.selected_file['display_name'])
    with col2:
        st.metric("Fields to Extract", len(st.session_state.extraction_fields))

if st.button("🚀 Extract Data from PDF", type="primary", disabled=not can_extract):
    with st.spinner("Extracting data from PDF... This may take a moment."):
        try:
            selected_file_name = st.session_state.selected_file['display_name']
            
            # Parse the document
            st.info("🔍 Parsing PDF document with AI...")
            
            # Try different approaches to parse the document
            parsed_text = None
            parse_error = None
            
            # Approach 1: Try with @ prefix in BUILD_SCOPED_FILE_URL
            try:
                parse_query = f"""
                SELECT 
                    SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                        '@{stage_name}', '{selected_file_name}'
                    ) as parsed_content
                """
                parse_result = session.sql(parse_query).collect()
                parsed_text = str(parse_result[0]['PARSED_CONTENT'])
                st.info(f"✓ Document parsed successfully ({len(parsed_text)} characters)")
            except Exception as e1:
                parse_error = str(e1)
                
                # Approach 2: Try reading file content directly and parsing
                try:
                    st.info("Trying alternative parsing method...")
                    
                    # Use GET_PRESIGNED_URL if available
                    parse_query = f"""
                    SELECT 
                        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                            GET_PRESIGNED_URL(@{stage_name}, '{selected_file_name}')
                        ) as parsed_content
                    """
                    parse_result = session.sql(parse_query).collect()
                    parsed_text = str(parse_result[0]['PARSED_CONTENT'])
                    st.info(f"✓ Document parsed successfully ({len(parsed_text)} characters)")
                except Exception as e2:
                    # Approach 3: Try with stage path format
                    try:
                        st.info("Trying stage path format...")
                        parse_query = f"""
                        SELECT 
                            SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                                '@{stage_name}/{selected_file_name}'
                            ) as parsed_content
                        """
                        parse_result = session.sql(parse_query).collect()
                        parsed_text = str(parse_result[0]['PARSED_CONTENT'])
                        st.info(f"✓ Document parsed successfully ({len(parsed_text)} characters)")
                    except Exception as e3:
                        # All approaches failed
                        st.error(f"Error parsing document with all methods")
                        st.error(f"Method 1 error: {str(e1)[:200]}")
                        st.error(f"Method 2 error: {str(e2)[:200]}")
                        st.error(f"Method 3 error: {str(e3)[:200]}")
                        
                        with st.expander("🔍 Troubleshooting Information"):
                            st.markdown("""
                            **Possible causes:**
                            1. PARSE_DOCUMENT function signature changed in your Snowflake version
                            2. File path format is incorrect
                            3. Cortex AI features not fully available
                            
                            **Try these steps:**
                            1. Verify file is in stage:
                            ```sql
                            LIST @{stage};
                            ```
                            
                            2. Test PARSE_DOCUMENT directly in SQL:
                            ```sql
                            SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                                BUILD_SCOPED_FILE_URL('@{stage}', '{file}')
                            );
                            ```
                            
                            3. Check Cortex AI availability:
                            ```sql
                            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama2-70b-chat', 'test');
                            ```
                            """.format(stage=stage_name, file=selected_file_name))
                        st.stop()
            
            if parsed_text is None:
                st.error("Failed to parse document")
                st.stop()
            
            # Now extract each field
            st.info("🤖 Extracting fields with AI...")
            extracted_values = {}
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, field in enumerate(st.session_state.extraction_fields):
                progress = (idx + 1) / len(st.session_state.extraction_fields)
                progress_bar.progress(progress)
                status_text.text(f"Extracting: {field['name']}...")
                
                #question = f"Extract the {field['name']}"
                #if field['description']:
                #    question += f" ({field['description']})"
                #question += " from this document. Provide only the extracted value, no explanation."
                
                question = f"{field['name']}"
                if field['description']:
                    question += f" ({field['description']})"
                question += ""

                # Escape quotes for SQL
                question_escaped = question.replace("'", "''")
                # Truncate parsed text if too long (avoid query size limits)
                parsed_text_truncated = parsed_text[:100000] if len(parsed_text) > 100000 else parsed_text
                parsed_text_escaped = parsed_text_truncated.replace("'", "''")

                extract_answer_query = f"""
                SELECT
                     AI_EXTRACT(
                     text => '{parsed_text_escaped}',
                     responseFormat => {{ '{field['name']}': '{field['description']}'}}
                     ):response:"{field['name']}"   as extracted_value
                """
                
                #extract_answer_query = f"""
                #SELECT 
                #    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
                #        '{parsed_text_escaped}',
                #        '{question_escaped}'
                #    ) as extracted_value
                #"""
                
                try:
                    result = session.sql(extract_answer_query).collect()
                    extracted_values[field['name']] = result[0]['EXTRACTED_VALUE']
                except Exception as e:
                    error_msg = str(e)
                    if len(error_msg) > 100:
                        error_msg = error_msg[:100] + "..."
                    extracted_values[field['name']] = f"[Error: {error_msg}]"
            
            progress_bar.progress(1.0)
            status_text.text("✅ Extraction complete!")
            
            # Store results in session state
            st.session_state.extracted_data = pd.DataFrame([extracted_values])
            
            st.success("✅ Data extraction complete!")
            st.balloons()
            st.experimental_rerun()
            
        except Exception as e:
            st.error(f"❌ Error during extraction: {str(e)}")
            with st.expander("🔍 View Full Error Details"):
                st.exception(e)
                st.info("💡 Tips:")
                st.markdown("""
                - Ensure your Snowflake account supports Cortex AI
                - Verify you have permissions to use PARSE_DOCUMENT and EXTRACT_ANSWER
                - Check that the stage exists and files are accessible
                - Try with a smaller PDF file first
                """)

# Display extracted data
if st.session_state.extracted_data is not None:
    st.markdown("---")
    st.header("📊 Step 4: Review & Download Results")
    
    # Display as dataframe
    st.dataframe(
        st.session_state.extracted_data,
        use_container_width=True
    )
    
    # Allow editing - using text inputs for Streamlit 1.22.0 compatibility
    with st.expander("✏️ Edit Extracted Data"):
        st.info("You can edit the extracted values below if needed:")
        
        edited_values = {}
        for col in st.session_state.extracted_data.columns:
            current_value = st.session_state.extracted_data[col].iloc[0]
            edited_values[col] = st.text_input(
                col,
                value=str(current_value),
                key=f"edit_{col}"
            )
        
        if st.button("💾 Save Edits"):
            st.session_state.extracted_data = pd.DataFrame([edited_values])
            st.success("Changes saved!")
            st.experimental_rerun()
    
    # Download section
    st.subheader("💾 Download Results")
    
    col1, col2 = st.columns([2, 2])
    
    with col1:
        # CSV download
        csv_buffer = io.StringIO()
        st.session_state.extracted_data.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        st.download_button(
            label="📥 Download as CSV",
            data=csv_data,
            file_name=f"extracted_data_{st.session_state.selected_file['display_name'].replace('.pdf', '')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        # JSON download
        json_data = st.session_state.extracted_data.to_json(orient='records', indent=2)
        
        st.download_button(
            label="📥 Download as JSON",
            data=json_data,
            file_name=f"extracted_data_{st.session_state.selected_file['display_name'].replace('.pdf', '')}.json",
            mime="application/json",
            use_container_width=True
        )
    
    # Note about Excel export
    st.info("💡 Tip: CSV files can be opened directly in Excel")
    
    # Batch processing option
    st.markdown("---")
    with st.expander("🔄 Batch Processing"):
        st.info("Process multiple PDFs with the same extraction fields")
        st.markdown("""
        To process multiple PDFs:
        1. Keep your extraction fields defined
        2. Select and process the first PDF
        3. Select and process additional PDFs
        4. Results will be added to a combined dataset
        """)
        
        if st.button("➕ Process Another PDF (Keep Current Data)"):
            st.info("Select another PDF from the list above to add to your dataset")
        
        if st.button("🗑️ Clear All Data"):
            st.session_state.extracted_data = None
            st.experimental_rerun()

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>Powered by Snowflake Cortex AI | Built with Streamlit</p>
    <p><small>This app uses PARSE_DOCUMENT and AI_EXTRACT functions from Snowflake Cortex</small></p>
    <p><small>Compatible with Streamlit 1.22.0+</small></p>
</div>
""", unsafe_allow_html=True)
