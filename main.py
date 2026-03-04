import streamlit as st
import pandas as pd
import requests
# from pages.Usage_Prep_Tool import *
# from helper.data import *
# from helper.graphs import *
# from helper.tabs_api import *
import time
import re
from datetime import datetime
from api import *
from usage_transformation import (
    price_book_transformation,
    tabs_billing_terms_format,
    tabs_billing_terms_to_upload,
    enterprise_support,
    prepaid,
    create_contracts,
    create_invoices,
    create_tabs_ready_usage,
    generate_prepaid_report_data,
    generate_commit_consumption_data
)

# Page configuration
st.set_page_config(
    page_title="Alkira Usage Transformation App",
    page_icon="🏍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

def check_api_key(api_key):
    """
    Validate API key by making a test API call.
    
    Args:
        api_key: The API key to validate
        
    Returns:
        bool: True if API key is valid, False otherwise
    """
    if not api_key or not api_key.strip():
        return False
    
    try:
        # Use a lightweight endpoint that doesn't require parameters
        # Using /v3/events/types which is already used successfully in the codebase
        url = "https://integrators.prod.api.tabsplatform.com/v3/events/types?limit=1"
        headers = {
            "Authorization": api_key.strip()
        }
        response = requests.get(url, headers=headers, timeout=10)
        # Accept any 2xx status code as success
        is_valid = 200 <= response.status_code < 300
        if not is_valid:
            # Debug: print the actual status code and response for troubleshooting
            print(f"API key validation failed: Status {response.status_code}")
            try:
                error_data = response.json()
                print(f"Response: {error_data}")
            except:
                print(f"Response text: {response.text[:200]}")
        return is_valid
    except Exception as e:
        # Debug: print the exception for troubleshooting
        print(f"API key validation exception: {str(e)}")
        return False

def show_authentication():
    """Display authentication screen for API key input."""
    st.title("🔐 Alkira Usage Uploader - Authentication")
    st.markdown("---")
    
    st.info("Please enter your Tabs API key to access the application.")
    
    # API key input
    api_key = st.text_input(
        "API Key",
        type="password",
        key="api_key_input",
        help="Enter your Tabs API key",
        placeholder="Enter your API key here"
    )
    
    # Submit button
    if st.button("Submit", type="primary", key="submit_api_key"):
        if api_key and api_key.strip():
            with st.spinner("Validating API key..."):
                if check_api_key(api_key):
                    st.session_state['tabs_api_key'] = api_key.strip()
                    st.session_state['authenticated'] = True
                    st.success("✓ API key validated successfully!")
                    st.rerun()
                else:
                    st.error("❌ Invalid API key. Please check your API key and try again.")
        else:
            st.warning("Please enter an API key.")
    
    st.markdown("---")
    st.caption("Contact your Tabs account manager via Slack if you need assistance.")

# Main app
def main():
    st.title("🏍️ Alkira Usage Uploader")
    st.markdown("---")
    
    # Main content area
    st.header("Usage Transformation")
    st.write("This is a custom built app for Alkira's Usage Billing. Upload the required files to process usage transformation. Complete all steps before clicking the Process Files button and do not click the button repeatedly!")
    st.write(" Contact your Tabs account manager via Slack if you have any questions.")

    # Billing run date input with button submission
    st.markdown("---")
    st.subheader("Billing Run Date")
    billing_date_input = st.text_input(
        "Enter Service Period Start Date for the Usage Billing (YYYY-MM-DD)",
        value=st.session_state.get('billing_run_date', ''),
        key="billing_date_input",
        help="Enter the billing run date in YYYY-MM-DD format (e.g., 2024-01-15)",
        placeholder="YYYY-MM-DD"
    )
    if st.button("Submit Billing Run Date", key="submit_billing_date", type="primary"):
        if billing_date_input:
            # Validate date format
            date_pattern = r'^\d{4}-\d{2}-\d{2}$'
            if re.match(date_pattern, billing_date_input):
                try:
                    # Validate that it's a valid date
                    datetime.strptime(billing_date_input, '%Y-%m-%d')
                    st.session_state['billing_run_date'] = billing_date_input
                    st.success(f"✓ Billing Run Date set to: {billing_date_input}")
                except ValueError:
                    st.error("Invalid date. Please enter a valid date in YYYY-MM-DD format.")
            else:
                st.error("Invalid format. Please enter the date in YYYY-MM-DD format (e.g., 2024-01-15).")
        else:
            st.warning("Please enter a billing run date before submitting.")
    
    # Display current billing run date if set
    if st.session_state.get('billing_run_date'):
        st.info(f"Current Billing Run Date: **{st.session_state['billing_run_date']}**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. Price Book ZIP")
        price_book_file = st.file_uploader(
            "Upload Price Book ZIP file",
            type=['zip'],
            key="price_book",
            help="Upload the Price Book ZIP file"
        )
        if price_book_file is not None:
            st.success(f"✓ Uploaded: {price_book_file.name}")
            st.session_state['price_book_file'] = price_book_file
            
            # Process the zip file to extract customer IDs
            # Get billing_run_date from session state if available
            billing_run_date = st.session_state.get('billing_run_date', None)
            # Check if we need to reprocess: new file, different file name, or billing_run_date changed
            last_billing_date = st.session_state.get('last_billing_run_date_used', None)
            needs_reprocess = (
                'price_book_data' not in st.session_state or 
                st.session_state.get('price_book_file_name') != price_book_file.name or
                last_billing_date != billing_run_date
            )
            if needs_reprocess:
                with st.spinner("Extracting customer IDs from ZIP file..."):
                    customer_files = price_book_transformation(price_book_file, billing_run_date)
                    st.session_state['price_book_data'] = customer_files
                    st.session_state['price_book_file_name'] = price_book_file.name
                    st.session_state['last_billing_run_date_used'] = billing_run_date
            
            # Display errors if any
            if st.session_state.get('price_book_data') and 'errors' in st.session_state['price_book_data']:
                errors = st.session_state['price_book_data']['errors']
                if errors:
                    with st.expander(f"⚠️ Processing Errors ({len(errors)} error(s))", expanded=True):
                        for error in errors:
                            st.error(error)
            
            # Display extracted customer IDs
            if st.session_state.get('price_book_data'):
                # Filter out 'combined', 'filtered', and 'errors' keys to get actual customer IDs
                customer_ids = [k for k in st.session_state['price_book_data'].keys() 
                              if k not in ['combined', 'filtered', 'errors']]
                if customer_ids:
                    st.info(f"Found {len(customer_ids)} customer(s): {', '.join(sorted(customer_ids, key=int))}")
    
    with col2:
        st.subheader("2. Alkira Raw Monthly Usage")
        raw_monthly_usage_file = st.file_uploader(
            "Upload Raw Monthly Usage file",
            type=['csv', 'xlsx', 'xls'],
            key="raw_monthly_usage",
            help="Upload the Raw Monthly Usage file"
        )
        if raw_monthly_usage_file is not None:
            st.success(f"✓ Uploaded: {raw_monthly_usage_file.name}")
            st.session_state['raw_monthly_usage_file'] = raw_monthly_usage_file
    
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("3. Enterprise Support Customers")
        enterprise_support_file = st.file_uploader(
            "Upload Enterprise Support file",
            type=['csv', 'xlsx', 'xls'],
            key="enterprise_support",
            help="Upload the Enterprise Support file"
        )
        if enterprise_support_file is not None:
            st.success(f"✓ Uploaded: {enterprise_support_file.name}")
            st.session_state['enterprise_support_file'] = enterprise_support_file
    
    with col4:
        st.subheader("4. Prepaid Customers")
        prepaid_file = st.file_uploader(
            "Upload Prepaid file",
            type=['csv', 'xlsx', 'xls'],
            key="prepaid",
            help="Upload the Prepaid file"
        )
        if prepaid_file is not None:
            st.success(f"✓ Uploaded: {prepaid_file.name}")
            st.session_state['prepaid_file'] = prepaid_file
    
    # Process button
    st.markdown("---")
    if st.button("Process Files", type="primary"):
        # Check if price book file is uploaded (required)
        if not st.session_state.get('price_book_file'):
            st.error("Please upload a Price Book ZIP file to process.")
        elif not st.session_state.get('billing_run_date'):
            st.warning("Please set a Billing Run Date before processing.")
        else:
            # Get billing_run_date from session state
            billing_run_date = st.session_state.get('billing_run_date')
            
            # Get uploaded files
            price_book_file = st.session_state.get('price_book_file')
            raw_monthly_usage_file = st.session_state.get('raw_monthly_usage_file')
            enterprise_support_file = st.session_state.get('enterprise_support_file')
            prepaid_file = st.session_state.get('prepaid_file')
            
            # Initialize result storage
            results = {}
            errors = []
            
            try:
                # Step 1: Process Price Book ZIP
                with st.spinner("Step 1/5: Processing Price Book ZIP file..."):
                    customer_files = price_book_transformation(price_book_file, billing_run_date)
                    
                    if 'errors' in customer_files and customer_files['errors']:
                        errors.extend(customer_files['errors'])
                    
                    if 'filtered' not in customer_files or customer_files['filtered'].empty:
                        st.error("Failed to process Price Book ZIP file. No filtered data available.")
                    else:
                        filtered_df = customer_files['filtered']
                        st.success(f"✓ Step 1: Processed Price Book ZIP ({len(filtered_df)} rows)")
                        results['filtered_df'] = filtered_df
                        
                        # Step 2: Filter with Raw Monthly Usage (if uploaded)
                        tabs_bt_clean_df = filtered_df
                        if raw_monthly_usage_file:
                            with st.spinner("Step 2/5: Filtering with Raw Monthly Usage file..."):
                                # Reset file pointer in case it was read before
                                raw_monthly_usage_file.seek(0)
                                tabs_bt_clean_df = tabs_billing_terms_to_upload(filtered_df, raw_monthly_usage_file, st)
                                st.success(f"✓ Step 2: Filtered with Raw Monthly Usage ({len(tabs_bt_clean_df)} rows)")
                        else:
                            st.info("Step 2: Skipped (Raw Monthly Usage file not uploaded)")
                        
                        # Step 3: Add Enterprise Support (if uploaded)
                        tabs_bt_enterprise = tabs_bt_clean_df
                        if enterprise_support_file:
                            with st.spinner("Step 3/5: Adding Enterprise Support rows..."):
                                tabs_bt_enterprise = enterprise_support(tabs_bt_clean_df, enterprise_support_file, billing_run_date)
                                st.success(f"✓ Step 3: Added Enterprise Support rows ({len(tabs_bt_enterprise)} rows)")
                        else:
                            st.info("Step 3: Skipped (Enterprise Support file not uploaded)")
                        
                        # Step 4: Add Prepaid (if uploaded)
                        tabs_bt_prepaid_enterprise = tabs_bt_enterprise
                        if prepaid_file:
                            with st.spinner("Step 4/5: Adding Prepaid rows..."):
                                tabs_bt_prepaid_enterprise = prepaid(tabs_bt_enterprise, prepaid_file, billing_run_date)
                                st.success(f"✓ Step 4: Added Prepaid rows ({len(tabs_bt_prepaid_enterprise)} rows)")
                        else:
                            st.info("Step 4: Skipped (Prepaid file not uploaded)")
                        
                        # Store the processed billing terms data (before contract/invoice creation)
                        results['tabs_bt_prepaid_enterprise'] = tabs_bt_prepaid_enterprise
                        results['billing_run_date'] = billing_run_date
                        
                        # Step 5: Create Tabs Ready Usage (if raw_monthly_usage_file uploaded)
                        usage_output = None
                        if raw_monthly_usage_file:
                            with st.spinner("Step 5/5: Creating Tabs Ready Usage file..."):
                                # Reset file pointer in case it was read before
                                raw_monthly_usage_file.seek(0)
                                if enterprise_support_file:
                                    enterprise_support_file.seek(0)
                                usage_output = create_tabs_ready_usage(
                                    raw_monthly_usage_file, 
                                    tabs_bt_prepaid_enterprise,  # Use prepaid data, not contracts
                                    enterprise_support_file, 
                                    billing_run_date,
                                    st  # Pass streamlit for debug output
                                )
                                if not usage_output.empty:
                                    st.success(f"✓ Step 5: Created Tabs Ready Usage ({len(usage_output)} rows)")
                                    results['usage_output'] = usage_output
                                    
                                    # Filter billing terms to only include rows with usage data
                                    # Keep: rows with usage + Enterprise Support + Prepaid
                                    with st.spinner("Filtering billing terms based on usage data..."):
                                        # Get unique (customer_id, SKU name) from usage output
                                        usage_keys = set(zip(
                                            usage_output['customer_id'],
                                            usage_output['event_type_name'].str.lower()
                                        ))
                                        
                                        # Filter billing terms
                                        def should_keep_row(row):
                                            customer_id = row.get('customer_id')
                                            name = str(row.get('name', '')).lower()
                                            
                                            # Keep Enterprise Support and Prepaid regardless
                                            if 'enterprise support' in name or 'prepaid' in name:
                                                return True
                                            
                                            # Keep if usage data exists
                                            return (customer_id, name) in usage_keys
                                        
                                        filtered_bt = tabs_bt_prepaid_enterprise[
                                            tabs_bt_prepaid_enterprise.apply(should_keep_row, axis=1)
                                        ].copy()
                                        
                                        removed_count = len(tabs_bt_prepaid_enterprise) - len(filtered_bt)
                                        st.success(f"✓ Filtered billing terms: keeping {len(filtered_bt)} rows (removed {removed_count} rows without usage)")
                                        
                                        # Update the billing terms to only include rows with usage
                                        results['tabs_bt_prepaid_enterprise'] = filtered_bt
                                else:
                                    st.warning("Step 5: Usage output is empty")
                        else:
                            st.info("Step 5: Skipped (Raw Monthly Usage file not uploaded)")
                        
                        # Store results in session state
                        st.session_state['processing_results'] = results
                        st.session_state['processing_errors'] = errors
                        
                        # Display errors if any
                        if errors:
                            with st.expander(f"⚠️ Processing Errors ({len(errors)} error(s))", expanded=False):
                                for error in errors:
                                    st.error(error)
                        
                        # Display success message
                        st.success("✅ Processing completed successfully!")
            
            except Exception as e:
                st.error(f"Error during processing: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
    
    # Create Contracts + Invoices in Tabs button (only shown after Process Files completes)
    if st.session_state.get('processing_results'):
        results = st.session_state['processing_results']
        
        # Check if we have processed data ready for contract creation
        if 'tabs_bt_prepaid_enterprise' in results:
            st.markdown("---")
            st.subheader("📤 Push to Tabs API")
            st.write("Create contracts and invoice obligations in Tabs. This step pushes data to the Tabs API.")
            
            if st.button("Create Contracts + Invoices in Tabs", type="primary", key="create_contracts_invoices"):
                tabs_bt_contract = None
                invoices_result = None
                
                try:
                    tabs_bt_prepaid_enterprise = results['tabs_bt_prepaid_enterprise']
                    billing_run_date = results.get('billing_run_date')
                    
                    # Step 1: Create Contracts
                    try:
                        with st.spinner("Creating contracts in Tabs..."):
                            tabs_bt_contract = create_contracts(tabs_bt_prepaid_enterprise, st)
                            contracts_created = tabs_bt_contract['contract_id'].notna().sum() if 'contract_id' in tabs_bt_contract.columns else 0
                            st.success(f"✓ Contract creation completed: {contracts_created} contracts created")
                            results['tabs_bt_contract'] = tabs_bt_contract
                    except Exception as contract_error:
                        st.error(f"❌ Error during contract creation: {str(contract_error)}")
                        import traceback
                        with st.expander("View Error Details"):
                            st.code(traceback.format_exc())
                        # Continue to invoices even if contracts failed
                        if tabs_bt_contract is None:
                            tabs_bt_contract = tabs_bt_prepaid_enterprise
                    
                    # Step 2: Create Invoices (continue even if some contracts failed)
                    if tabs_bt_contract is not None:
                        try:
                            with st.spinner("Creating invoices and pushing to Tabs API..."):
                                invoices_result = create_invoices(tabs_bt_contract, st)
                                success_count = (invoices_result['push_status'] == 'SUCCESS').sum() if 'push_status' in invoices_result.columns else 0
                                st.success(f"✓ Invoice creation completed: {success_count} invoices created")
                                results['invoices_result'] = invoices_result
                        except Exception as invoice_error:
                            st.error(f"❌ Error during invoice creation: {str(invoice_error)}")
                            import traceback
                            with st.expander("View Error Details"):
                                st.code(traceback.format_exc())
                    else:
                        st.warning("⚠️ Skipping invoice creation due to contract creation failure")
                    
                    # Update session state
                    st.session_state['processing_results'] = results
                    
                    # Show final status
                    if tabs_bt_contract is not None and invoices_result is not None:
                        st.success("✅ Contracts and Invoices process completed!")
                    elif tabs_bt_contract is not None:
                        st.warning("⚠️ Contracts created but invoices failed. Check errors above.")
                    else:
                        st.error("❌ Both contracts and invoices failed. Check errors above.")
                    
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Unexpected error: {str(e)}")
                    import traceback
                    with st.expander("View Error Details"):
                        st.code(traceback.format_exc())
    
    # Display CSV outputs if processing was completed
    if st.session_state.get('processing_results'):
        st.markdown("---")
        st.subheader("📥 Download Output Files")
        results = st.session_state['processing_results']
        
        # Billing Terms CSV
        # Use invoices_result if contracts have been created, otherwise use tabs_bt_prepaid_enterprise
        billing_terms_df = None
        if 'invoices_result' in results and not results['invoices_result'].empty:
            billing_terms_df = results['invoices_result']
            csv_title = "Billing Terms CSV (with Contract IDs & Push Status)"
        elif 'tabs_bt_prepaid_enterprise' in results and not results['tabs_bt_prepaid_enterprise'].empty:
            billing_terms_df = results['tabs_bt_prepaid_enterprise']
            csv_title = "Billing Terms CSV (Processed Data)"
        
        if billing_terms_df is not None:
            st.markdown(f"### {csv_title}")
            st.info(f"Rows: {len(billing_terms_df)} | Columns: {len(billing_terms_df.columns)}")
            
            # Convert to CSV
            csv_billing = billing_terms_df.to_csv(index=False)
            
            # Display preview
            with st.expander("Preview Billing Terms Data"):
                st.dataframe(billing_terms_df.head(20))
            
            # Download button
            st.download_button(
                label="Download Billing Terms CSV",
                data=csv_billing,
                file_name="billing_terms.csv",
                mime="text/csv"
            )
        
        # Usage CSV
        if 'usage_output' in results:
            usage_df = results['usage_output']
            if not usage_df.empty:
                st.markdown("### Usage CSV")
                st.info(f"Rows: {len(usage_df)} | Columns: {len(usage_df.columns)}")
                
                # Convert to CSV (exclude event_type_id, keep only event_type_name)
                columns_to_export = ['customer_id', 'event_type_name', 'datetime', 'value', 'differentiator', 'invoice']
                csv_usage = usage_df[columns_to_export].to_csv(index=False)
                
                # Display preview
                with st.expander("Preview Usage Data"):
                    st.dataframe(usage_df.head(20))
                
                # Download button
                st.download_button(
                    label="Download Usage CSV",
                    data=csv_usage,
                    file_name="tabs_ready_usage.csv",
                    mime="text/csv"
                )
                
                # Push Usage Events to Tabs button
                st.markdown("### Push Usage Events to Tabs")
                if st.button("Push Usage Events to Tabs", type="primary"):
                    try:
                        from api import create_usage_events_bulk
                        
                        # Exclude Prepaid rows from usage events push
                        filtered_usage = usage_df[~usage_df['event_type_name'].str.contains('Prepaid', case=False, na=False)]
                        
                        # Convert filtered usage to list of event dictionaries
                        events_list = []
                        for _, row in filtered_usage.iterrows():
                            # invoice column already contains the correct format (blank, 1, 2, 3...)
                            # Just use it directly as invoice_split_key
                            invoice_split_key = str(row.get('invoice', ''))
                            
                            event = {
                                'customer_id': str(row.get('customer_id', '')),
                                'event_type_id': str(row.get('event_type_id', '')),
                                'datetime': str(row.get('datetime', '')),
                                'value': float(row.get('value', 0)) if pd.notna(row.get('value')) else 0,
                                'differentiator': str(row.get('differentiator', '')),
                                'invoice_split_key': invoice_split_key
                            }
                            events_list.append(event)
                        
                        if events_list:
                            with st.spinner(f"Pushing {len(events_list)} usage events to Tabs..."):
                                result = create_usage_events_bulk(events_list)
                            
                            # Check success/failure counts from bulk response
                            success_count = result.get('success_count', 0)
                            failure_count = result.get('failure_count', 0)
                            total = result.get('total', 0)
                            
                            if failure_count == 0:
                                st.success(f"✅ Successfully pushed {success_count}/{total} usage events to Tabs")
                                
                                # Mark all unique contracts as processed (excluding Prepaid)
                                try:
                                    from api import mark_contract_processed
                                    tabs_bt_contract = results.get('tabs_bt_contract')
                                    
                                    if tabs_bt_contract is not None and not tabs_bt_contract.empty:
                                        # Filter out Prepaid rows
                                        non_prepaid_bt = tabs_bt_contract[~tabs_bt_contract['name'].str.contains('Prepaid', case=False, na=False)]
                                        unique_contracts = non_prepaid_bt['contract_id'].dropna().unique()
                                        
                                        mark_success = 0
                                        mark_fail = 0
                                        
                                        with st.spinner(f"Marking {len(unique_contracts)} contract(s) as processed..."):
                                            for contract_id in unique_contracts:
                                                if contract_id and str(contract_id) != 'nan' and str(contract_id) != '':
                                                    result = mark_contract_processed(str(contract_id))
                                                    if result.get('success'):
                                                        mark_success += 1
                                                    else:
                                                        mark_fail += 1
                                        
                                        if mark_fail == 0:
                                            st.success(f"✅ Marked {mark_success} contract(s) as processed")
                                        else:
                                            st.warning(f"⚠️ Marked {mark_success} contract(s). {mark_fail} failed.")
                                except Exception as mark_error:
                                    st.warning(f"⚠️ Usage events pushed but failed to mark contracts: {str(mark_error)}")
                                    
                            elif success_count > 0:
                                st.warning(f"⚠️ Pushed {success_count}/{total} events. {failure_count} failed.")
                                if result.get('failures'):
                                    with st.expander("View Errors"):
                                        for failure in result.get('failures', []):
                                            st.error(f"Event {failure.get('index')}: {failure.get('error')}")
                            else:
                                st.error(f"❌ Failed to push events. {failure_count}/{total} failed.")
                                if result.get('failures'):
                                    with st.expander("View Errors"):
                                        for failure in result.get('failures', []):
                                            st.error(f"Event {failure.get('index')}: {failure.get('error')}")
                        else:
                            st.warning("No usage events to push")
                    except Exception as e:
                        st.error(f"Error pushing usage events: {str(e)}")
                        import traceback
                        st.code(traceback.format_exc())
                
                # Apply Prepaid Button
                st.markdown("### Apply Prepaid")
                if st.button("Apply Prepaid", type="primary"):
                    try:
                        from api import mark_contract_processed, get_invoices, create_usage_event
                        
                        # Get tabs_bt_contract from results
                        tabs_bt_contract = results.get('tabs_bt_contract')
                        
                        if tabs_bt_contract is None or tabs_bt_contract.empty:
                            st.error("No billing terms data found")
                        else:
                            # Find Prepaid rows in tabs_bt_contract
                            prepaid_mask = tabs_bt_contract['name'].str.contains('Prepaid', case=False, na=False)
                            prepaid_billing_terms = tabs_bt_contract[prepaid_mask]
                            
                            if prepaid_billing_terms.empty:
                                st.warning("No Prepaid billing terms found")
                            else:
                                success_count = 0
                                failure_count = 0
                                errors = []
                                
                                with st.spinner(f"Processing {len(prepaid_billing_terms)} Prepaid customer(s)..."):
                                    for _, prepaid_row in prepaid_billing_terms.iterrows():
                                        customer_id = str(prepaid_row.get('customer_id', ''))
                                        contract_id = str(prepaid_row.get('contract_id', ''))
                                        # Get the event_to_track (Prepaid event type ID) from the billing term row
                                        prepaid_event_type_id = str(prepaid_row.get('event_to_track', ''))
                                        
                                        if not customer_id or not contract_id or customer_id == 'nan' or contract_id == 'nan':
                                            errors.append(f"Missing customer_id or contract_id for row")
                                            failure_count += 1
                                            continue
                                        
                                        if not prepaid_event_type_id or prepaid_event_type_id == 'nan':
                                            errors.append(f"Customer {customer_id}: Missing event_to_track (Prepaid event type ID)")
                                            failure_count += 1
                                            continue
                                        
                                        try:
                                            # Step 1: Get invoice total
                                            invoice_result = get_invoices(customer_id, contract_id)
                                            if not invoice_result.get('success'):
                                                errors.append(f"Customer {customer_id}: Failed to get invoices - {invoice_result.get('message')}")
                                                failure_count += 1
                                                continue
                                            
                                            invoice_total = invoice_result.get('total_amount', 0)
                                            
                                            # #region agent log
                                            import json
                                            log_path = '/Users/chiragdas/Documents/GitHub/Alkira_streamlit/debug.log'
                                            with open(log_path, 'a') as f:
                                                f.write(json.dumps({"id":"log_INVOICE","timestamp":__import__('time').time()*1000,"location":"main.py:566","message":"Invoice result for Prepaid","data":{"customer_id":customer_id,"contract_id":contract_id,"invoice_total":invoice_total,"invoice_total_type":str(type(invoice_total).__name__),"invoice_result_keys":list(invoice_result.keys())},"runId":"prepaid","hypothesisId":"A"}) + '\n')
                                                f.flush()
                                            # #endregion
                                            
                                            # Step 2: Push Prepaid usage event with invoice total
                                            billing_run_date = results.get('billing_run_date', datetime.now().strftime('%Y-%m-%d'))
                                            
                                            event_data = {
                                                'customer_id': customer_id,
                                                'event_type_id': prepaid_event_type_id,
                                                'datetime': billing_run_date,
                                                'value': invoice_total,
                                                'differentiator': '',
                                                'invoice_split_key': ''
                                            }
                                            
                                            # #region agent log
                                            with open(log_path, 'a') as f:
                                                f.write(json.dumps({"id":"log_EVENT_DATA","timestamp":__import__('time').time()*1000,"location":"main.py:578","message":"Event data before API call","data":{"event_data":event_data,"billing_run_date":billing_run_date,"billing_run_date_type":str(type(billing_run_date).__name__)},"runId":"prepaid","hypothesisId":"B,C,D,E"}) + '\n')
                                                f.flush()
                                            # #endregion
                                            
                                            event_result = create_usage_event(event_data)
                                            
                                            if not event_result.get('success'):
                                                errors.append(f"Customer {customer_id}: Failed to push Prepaid event - {event_result.get('message')}")
                                                failure_count += 1
                                                continue
                                            
                                            # Step 3: Mark contract as processed (last step)
                                            mark_result = mark_contract_processed(contract_id)
                                            if not mark_result.get('success'):
                                                errors.append(f"Customer {customer_id}: Prepaid event pushed but failed to mark contract as processed - {mark_result.get('message')}")
                                                # Still count as success since the prepaid event was pushed
                                                success_count += 1
                                            else:
                                                success_count += 1
                                                
                                        except Exception as row_error:
                                            errors.append(f"Customer {customer_id}: {str(row_error)}")
                                            failure_count += 1
                                
                                # Show results
                                if failure_count == 0:
                                    st.success(f"✅ Successfully applied Prepaid for {success_count} customer(s)")
                                elif success_count > 0:
                                    st.warning(f"⚠️ Applied Prepaid for {success_count} customer(s). {failure_count} failed.")
                                    with st.expander("View Errors"):
                                        for error in errors:
                                            st.error(error)
                                else:
                                    st.error(f"❌ Failed to apply Prepaid. {failure_count} failed.")
                                    with st.expander("View Errors"):
                                        for error in errors:
                                            st.error(error)
                                            
                    except ImportError as e:
                        st.error(f"Missing required API functions: {str(e)}")
                    except Exception as e:
                        st.error(f"Error applying Prepaid: {str(e)}")
                        import traceback
                        st.code(traceback.format_exc())
                
                # Google Sheets Update Buttons
                st.markdown("### Update Google Sheets Reports")
                st.write("Reports can be found here: https://docs.google.com/spreadsheets/d/10Znr32hQQRS1qOcVQIqAtg9PU_6ht5z7WjfXyaL47i4/edit?usp=sharing !")

                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("Update Prepaid Sheet", type="secondary"):
                        try:
                            from google_sheets import update_prepaid_sheet
                            
                            # Get billing terms data - prefer tabs_bt_contract if available
                            billing_terms_data = results.get('tabs_bt_contract')
                            if billing_terms_data is None or (hasattr(billing_terms_data, 'empty') and billing_terms_data.empty):
                                billing_terms_data = results.get('tabs_bt_prepaid_enterprise')
                            
                            if billing_terms_data is None or (hasattr(billing_terms_data, 'empty') and billing_terms_data.empty):
                                st.error("No billing terms data available")
                            else:
                                # Generate prepaid data
                                prepaid_data = generate_prepaid_report_data(usage_df, billing_terms_data)
                                
                                if prepaid_data:
                                    with st.spinner("Updating Prepaid Sheet..."):
                                        result = update_prepaid_sheet(prepaid_data)
                                    
                                    if result.get('success'):
                                        st.success(result.get('message'))
                                    else:
                                        st.error(result.get('message'))
                                else:
                                    st.warning("No prepaid data found to update")
                        except ImportError:
                            st.error("Google Sheets integration not configured. Please set up service account credentials.")
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
                
                with col2:
                    if st.button("Update Commit Consumption Sheet", type="secondary"):
                        try:
                            from google_sheets import update_commit_consumption_sheet
                            
                            # Get billing terms data - prefer tabs_bt_contract if available
                            billing_terms_data = results.get('tabs_bt_contract')
                            if billing_terms_data is None or (hasattr(billing_terms_data, 'empty') and billing_terms_data.empty):
                                billing_terms_data = results.get('tabs_bt_prepaid_enterprise')
                            billing_run_date = results.get('billing_run_date', datetime.now().strftime('%Y-%m-%d'))
                            
                            if billing_terms_data is None or (hasattr(billing_terms_data, 'empty') and billing_terms_data.empty):
                                st.error("No billing terms data available")
                            else:
                                # Generate consumption data
                                consumption_data = generate_commit_consumption_data(usage_df, billing_terms_data)
                                
                                if consumption_data:
                                    with st.spinner("Updating Commit Consumption Sheet..."):
                                        result = update_commit_consumption_sheet(consumption_data, billing_run_date)
                                    
                                    if result.get('success'):
                                        st.success(result.get('message'))
                                    else:
                                        st.error(result.get('message'))
                                else:
                                    st.warning("No consumption data found to update")
                        except ImportError:
                            st.error("Google Sheets integration not configured. Please set up service account credentials.")
                        except Exception as e:
                            st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    # Check if user is authenticated
    if not st.session_state.get('authenticated', False):
        show_authentication()
    else:
        main()
