from typing import Any
import zipfile
import io
import re
import json
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from api import get_all_customers, get_event_ids, get_integration_items, create_contract, push_bt

def price_book_transformation(zip_file, billing_run_date=None):
    """
    Extract ZIP file and parse tenant IDs from filenames.
    Supports ZIP files containing either CSV files or XLSX/XLS files (or both).
    
    Filename formats supported:
    - New format: 40_Koch_SFDC#00000190.xlsx (or .csv) - tenant_id is the number at the start (before first underscore)
    - Old format: price_by_sku_40_Koch_SFDC#00000190.xlsx (or .csv) - tenant_id is the number after 'price_by_sku_'
    
    Args:
        zip_file: UploadedFile object from Streamlit file_uploader (ZIP file containing CSV or XLSX/XLS files)
        billing_run_date: Optional billing run date in YYYY-MM-DD format to populate date columns
        
    Returns:
        dict: Dictionary with individual customer files, combined DataFrame, filtered DataFrame, and errors
        {
            tenant_id: {
                'filename': str,
                'data': pd.DataFrame
            },
            ...
            'combined': pd.DataFrame  # All data combined into one DataFrame
            'filtered': pd.DataFrame  # Filtered DataFrame with only: Tabs Customer ID, event_to_track, integration_item_id, SKU Name, NET RATE
            'errors': list  # List of error messages
        }
    """
    if zip_file is None:
        return {'errors': []}
    
    customer_files = {}
    all_dataframes = []  # List to collect all DataFrames for combination
    errors = []  # List to collect error messages
    
    try:
        # Read the zip file into memory
        zip_bytes = zip_file.read()
        zip_file_obj = zipfile.ZipFile(io.BytesIO(zip_bytes))
        
        # Get list of all files in the zip
        file_list = zip_file_obj.namelist()
        
        # Filter for both CSV and Excel files, excluding macOS metadata files
        data_files = [
            f for f in file_list 
            if f.endswith(('.csv', '.xlsx', '.xls'))
            and not f.startswith('__MACOSX/')  # macOS metadata folder
            and not f.split('/')[-1].startswith('._')  # macOS resource fork files
        ]
        
        # Parse each filename to extract tenant_id and contract_name
        for filename in data_files:
            # Extract just the basename (filename without path) for pattern matching
            basename = filename.split('/')[-1] if '/' in filename else filename
            
            # Extract tenant_id: Try new format first (digits at start), fallback to old format (after 'price_by_sku_')
            # Format: 356_Borgwarner_SFDC#00000323.xlsx or 227-SitaQantas_SFDC#00000312.xlsx
            tenant_match = re.search(r'^(\d+)[_-]', basename) or re.search(r'price_by_sku_(\d+)_', basename)
            
            # Extract contract_name: Pattern like SFDC#00000323 (before file extension)
            contract_match = re.search(r'(SFDC#\d+)\.\w+$', basename, re.IGNORECASE)
            
            if tenant_match:
                tenant_id = tenant_match.group(1)
                contract_name = contract_match.group(1) if contract_match else ''
                
                # Read the file from zip
                try:
                    file_data = zip_file_obj.read(filename)
                    
                    # Determine file type and read accordingly
                    file_extension = filename.split('.')[-1].lower()
                    if file_extension == 'csv':
                        # Read the CSV file
                        df_full = pd.read_csv(io.BytesIO(file_data))
                    else:
                        # Read the Excel file (xlsx or xls)
                        df_full = pd.read_excel(io.BytesIO(file_data))
                    
                    # Required columns (case-insensitive matching)
                    required_columns = ['Category', 'SKU Name', 'SKU Description', 'Unit of Measure', 
                                       'On-Demand Rate', 'Disc', 'NET RATE', 'Net Terms']
                    
                    # Create case-insensitive mapping (convert to string first for numeric column names)
                    df_columns_lower = {str(col).lower(): col for col in df_full.columns}
                    
                    # Find matching columns and check for missing ones
                    matched_columns = []
                    missing_columns = []
                    
                    # Special handling for Net Terms - accept variations: "Net Terms", "Terms", or "Term"
                    net_terms_variations = ['net terms', 'terms', 'term']
                    net_terms_found = None
                    for variation in net_terms_variations:
                        if variation in df_columns_lower:
                            net_terms_found = df_columns_lower[variation]
                            break
                    
                    for req_col in required_columns:
                        req_col_lower = req_col.lower()
                        if req_col_lower == 'net terms':
                            # Handle Net Terms variations
                            if net_terms_found:
                                matched_columns.append(net_terms_found)
                            else:
                                missing_columns.append(req_col)
                        elif req_col_lower in df_columns_lower:
                            matched_columns.append(df_columns_lower[req_col_lower])
                        else:
                            missing_columns.append(req_col)
                    
                    # Raise error if any columns are missing
                    if missing_columns:
                        error_msg = f"Error in {filename}: Missing required columns: {', '.join(missing_columns)}"
                        print(error_msg)
                        errors.append(error_msg)
                        raise ValueError(error_msg)
                    
                    # Select only the matched columns
                    df = df_full[matched_columns].copy()
                    
                    # Normalize Net Terms column name if it was found with a variation
                    if net_terms_found and net_terms_found != 'Net Terms':
                        df = df.rename(columns={net_terms_found: 'Net Terms'})
                    
                    # Check for Excel formula errors in NET RATE column
                    if 'NET RATE' in df.columns:
                        # Convert to string to check for error values
                        net_rate_str = df['NET RATE'].astype(str).str.upper()
                        error_mask = net_rate_str.str.contains('#REF', na=False) | net_rate_str.str.contains('#N/A', na=False) | net_rate_str.str.contains('#VALUE', na=False) | net_rate_str.str.contains('#DIV/0', na=False)
                        if error_mask.any():
                            error_count = error_mask.sum()
                            error_skus = df.loc[error_mask, 'SKU Name'].unique() if 'SKU Name' in df.columns else []
                            print(f"Warning: Found {error_count} row(s) with Excel formula errors in NET RATE column in {filename}")
                            if len(error_skus) > 0:
                                print(f"  Affected SKU names: {', '.join(str(sku) for sku in error_skus[:10])}")
                                if len(error_skus) > 10:
                                    print(f"  ... and {len(error_skus) - 10} more SKU name(s)")
                    
                    # Add tenant_id column with the tenant_id value for every row
                    df['tenant_id'] = tenant_id
                    
                    # Add contract_name column with the contract name from filename
                    df['contract_name'] = contract_name
                    
                    # Add Tabs Customer ID column (to be populated later via API)
                    df['Tabs Customer ID'] = None
                    
                    # Add event_to_track column (to be populated later via API)
                    df['event_to_track'] = None
                    
                    # Add integration_item_id column (to be populated later via API)
                    df['integration_item_id'] = None
                    
                    # Store individual file data
                    customer_files[tenant_id] = {
                        'filename': filename,
                        'data': df,
                        'tenant_id': tenant_id,
                        'contract_name': contract_name
                    }
                    
                    # Add to list for combining
                    all_dataframes.append(df)
                except ValueError as e:
                    # Error message already printed, continue to next file
                    continue
                except Exception as e:
                    error_msg = f"Error reading {filename}: {str(e)}"
                    print(error_msg)
                    errors.append(error_msg)
                    continue
            else:
                error_msg = f"Could not extract customer_id from filename: {filename}"
                print(error_msg)
                errors.append(error_msg)
        
        zip_file_obj.close()
        
        # Get all customers from API to map tenant_id to Tabs Customer ID
        if all_dataframes:
            try:
                customers_data = get_all_customers()
                
                # Create mapping dictionary: tenant_id -> tabs_customer_id
                tenant_to_customer_id = {}
                for customer in customers_data:
                    tabs_customer_id = customer.get('id')
                    custom_fields = customer.get('customFields', [])
                    
                    # Find the Tenant ID custom field, fallback to Account # if not found
                    tenant_id_value = None
                    for field in custom_fields:
                        field_name = field.get('customFieldName')
                        if field_name == 'Tenant ID':
                            tenant_id_value = field.get('customFieldValue')
                            if tenant_id_value:
                                tenant_to_customer_id[tenant_id_value] = tabs_customer_id
                                break
                    
                    # If Tenant ID not found, try Account #
                    if not tenant_id_value:
                        for field in custom_fields:
                            if field.get('customFieldName') == 'Account #':
                                tenant_id_value = field.get('customFieldValue')
                                if tenant_id_value:
                                    tenant_to_customer_id[tenant_id_value] = tabs_customer_id
                                    break
                
                # Collect all unique tenant_ids from the files
                file_tenant_ids = set()
                for df in all_dataframes:
                    file_tenant_ids.update(df['tenant_id'].unique())
                
                # Check for unmatched tenant IDs
                unmatched_tenant_ids = file_tenant_ids - set(tenant_to_customer_id.keys())
                if unmatched_tenant_ids:
                    print(f"Warning: No matching Tabs Customer ID found for tenant ID(s): {', '.join(sorted(unmatched_tenant_ids, key=str))}")
                else:
                    print(f"Successfully matched all {len(file_tenant_ids)} tenant ID(s) to Tabs Customer IDs")
                
                # Update Tabs Customer ID in all DataFrames
                # Update DataFrames in all_dataframes list (they're the same objects as in customer_files)
                for df in all_dataframes:
                    df['Tabs Customer ID'] = df['tenant_id'].map(tenant_to_customer_id)
                
                # Get event IDs from API and populate event_to_track
                try:
                    events_df = get_event_ids()
                    
                    # Create mapping dictionary: event_name -> event_id (case-insensitive)
                    event_name_to_id = {}
                    if not events_df.empty and 'name' in events_df.columns and 'id' in events_df.columns:
                        for _, row in events_df.iterrows():
                            event_name = row.get('name')
                            event_id = row.get('id')
                            if event_name and event_id:
                                # Use lowercase as key for case-insensitive matching
                                event_name_to_id[event_name.lower()] = event_id
                    
                    # Update event_to_track in all DataFrames
                    for df in all_dataframes:
                        if 'SKU Name' in df.columns:
                            # Convert SKU Name to lowercase for case-insensitive matching
                            df['event_to_track'] = df['SKU Name'].str.lower().map(event_name_to_id)
                            
                            # Check for unmatched SKU Names
                            unmatched_skus = df[df['event_to_track'].isna() & df['SKU Name'].notna()]['SKU Name'].unique()
                            if len(unmatched_skus) > 0:
                                print(f"Warning: No matching event ID found for SKU Name(s): {', '.join(unmatched_skus[:10])}" + 
                                      (f" (and {len(unmatched_skus) - 10} more)" if len(unmatched_skus) > 10 else ""))
                    
                except Exception as e:
                    print(f"Error fetching event IDs from API: {str(e)}")
                
                # Get integration items from API and populate integration_item_id
                try:
                    items_df = get_integration_items()
                    
                    # Create mapping dictionary: item_name -> item_id
                    item_name_to_id = {}
                    if not items_df.empty and 'name' in items_df.columns and 'id' in items_df.columns:
                        for _, row in items_df.iterrows():
                            item_name = row.get('name')
                            item_id = row.get('id')
                            if item_name and item_id:
                                item_name_to_id[item_name] = item_id
                    
                    # Update integration_item_id in all DataFrames
                    for df in all_dataframes:
                        if 'SKU Name' in df.columns:
                            df['integration_item_id'] = df['SKU Name'].map(item_name_to_id)
                            
                            # Check for unmatched SKU Names
                            unmatched_items = df[df['integration_item_id'].isna() & df['SKU Name'].notna()]['SKU Name'].unique()
                            if len(unmatched_items) > 0:
                                print(f"Warning: No matching integration item ID found for SKU Name(s): {', '.join(unmatched_items[:10])}" + 
                                      (f" (and {len(unmatched_items) - 10} more)" if len(unmatched_items) > 10 else ""))
                    
                except Exception as e:
                    print(f"Error fetching integration items from API: {str(e)}")
                
                # Combine all DataFrames into one
                combined_df = pd.concat(all_dataframes, ignore_index=True)
                customer_files['combined'] = combined_df
                
                # Create filtered DataFrame using tabs_billing_terms_format function
                customer_files['filtered'] = tabs_billing_terms_format(combined_df, billing_run_date)
                
            except Exception as e:
                print(f"Error fetching customers from API: {str(e)}")
                # Still combine DataFrames even if API call fails
                combined_df = pd.concat(all_dataframes, ignore_index=True)
                customer_files['combined'] = combined_df
                
                # Create filtered DataFrame even if API calls failed
                customer_files['filtered'] = tabs_billing_terms_format(combined_df, billing_run_date)
        
    except Exception as e:
        error_msg = f"Error processing zip file: {str(e)}"
        print(error_msg)
        errors.append(error_msg)
        return {'errors': errors}
    
    # Add errors to the return dictionary
    customer_files['errors'] = errors
    return customer_files

def tabs_billing_terms_format(combined_df, billing_run_date=None):
    """
    Create a filtered DataFrame with only the columns needed for Tabs billing terms.
    
    Args:
        combined_df: Combined DataFrame from price_book_transformation
        billing_run_date: Optional billing run date in YYYY-MM-DD format to populate date columns
        
    Returns:
        pd.DataFrame: Filtered DataFrame with: Tabs Customer ID, event_to_track, 
                     integration_item_id, SKU Name, NET RATE, and additional hardcoded columns
    """
    filtered_columns = ['Tabs Customer ID', 'event_to_track', 'integration_item_id', 'SKU Name', 'NET RATE', 'tenant_id', 'contract_name', 'Net Terms']
    available_columns = [col for col in filtered_columns if col in combined_df.columns]
    filtered_df = combined_df[available_columns].copy()
    
    # Rename columns
    filtered_df = filtered_df.rename(columns={
        'Tabs Customer ID': 'customer_id',
        'SKU Name': 'name',
        'NET RATE': 'amount_1'
    })
    
    # Convert amount_1 to string to ensure consistent type (handles mixed numeric/string values)
    if 'amount_1' in filtered_df.columns:
        filtered_df['amount_1'] = filtered_df['amount_1'].astype(str)
    
    # Add hardcoded columns with values for each row
    filtered_df['contract_id'] = ''  # TODO: Update with actual value
    # Use billing_run_date if provided, otherwise 'FLAT'
    filtered_df['invoice_date'] = billing_run_date
    filtered_df['is_recurring'] = "TRUE"  
    filtered_df['quantity'] = 1  
    filtered_df['due_interval'] = 1 
    filtered_df['due_interval_unit'] = 'MONTH'  
    filtered_df['duration'] = 1 
    # Use Net Terms from the file if available, otherwise empty string
    if 'Net Terms' in filtered_df.columns:
        filtered_df['net_payment_terms'] = filtered_df['Net Terms'].fillna('').astype(str)
    else:
        filtered_df['net_payment_terms'] = ''
    filtered_df['is_volume'] = 'FALSE'  
    filtered_df['billing_type'] = 'UNIT_PRICE'  
    filtered_df['invoiceDateStrategy'] = 'ARREARS' 
    filtered_df['note'] = ''  
    # Use billing_run_date if provided, otherwise 'FLAT'
    filtered_df['revenue_start_date'] = billing_run_date 
    # Calculate revenue_end_date as billing_run_date + 30 days
    if billing_run_date:
        try:
            # Parse the billing_run_date
            date_obj = datetime.strptime(billing_run_date, '%Y-%m-%d')
            # Add 30 days
            end_date_obj = date_obj + timedelta(days=30)
            # Create date string for the end date
            revenue_end_date = end_date_obj.strftime('%Y-%m-%d')
            filtered_df['revenue_end_date'] = revenue_end_date
        except (ValueError, AttributeError):
            # If parsing fails, use the original billing_run_date
            filtered_df['revenue_end_date'] = billing_run_date
    else:
        filtered_df['revenue_end_date'] = billing_run_date
    filtered_df['invoice_type'] = 'INVOICE'  
    filtered_df['class_id'] = ''
    filtered_df['revenue_product_id'] = ''
    filtered_df['value_1'] = ''
    filtered_df['amount_2'] = ''
    filtered_df['value_2'] = ''
    
    # Reorder columns to match the required order
    column_order = [
        'customer_id',
        'tenant_id',
        'contract_id',
        'invoice_date',
        'is_recurring',
        'quantity',
        'due_interval',
        'due_interval_unit',
        'duration',
        'net_payment_terms',
        'is_volume',
        'billing_type',
        'invoiceDateStrategy',
        'event_to_track',
        'name',
        'note',
        'integration_item_id',
        'revenue_start_date',
        'revenue_end_date',
        'invoice_type',
        'revenue_product_id',
        'amount_1',
        'value_1',
        'amount_2',
        'value_2',
        'class_id'
    ]
    
    # Only include columns that exist in the DataFrame
    existing_columns = [col for col in column_order if col in filtered_df.columns]
    # Add any remaining columns that weren't in the order list (like class_id)
    remaining_columns = [col for col in filtered_df.columns if col not in column_order]
    filtered_df = filtered_df[existing_columns + remaining_columns]
    
    return filtered_df


def tabs_billing_terms_to_upload(filtered_df, raw_monthly_usage_file):
    """
    Compare the filtered_df with the Raw Monthly Usage file.
    Filter filtered_df to only include rows where tenant_id, name, and contract_name match
    the Tenant ID, SKU Name, and Contract from the raw monthly usage file.
    
    Args:
        filtered_df: Filtered DataFrame from tabs_billing_terms_format
        raw_monthly_usage_file: UploadedFile object from Streamlit file_uploader (CSV, XLSX, or XLS)
        
    Returns:
        pd.DataFrame: Filtered DataFrame (Tabs_bt_final_df) with only matching rows
    """
    if raw_monthly_usage_file is None:
        return filtered_df
    
    try:
        # Read the raw monthly usage file based on file extension
        file_extension = raw_monthly_usage_file.name.split('.')[-1].lower()
        
        if file_extension == 'csv':
            raw_usage_df = pd.read_csv(raw_monthly_usage_file)
        elif file_extension in ['xlsx', 'xls']:
            raw_usage_df = pd.read_excel(raw_monthly_usage_file)
        else:
            print(f"Unsupported file type: {file_extension}")
            return filtered_df
        
        # Check if required columns exist in raw usage file (including Contract/SFDC#)
        # Accept either 'Contract' or 'SFDC#' as the contract column name
        contract_col = 'Contract' if 'Contract' in raw_usage_df.columns else ('SFDC#' if 'SFDC#' in raw_usage_df.columns else None)
        
        required_columns = ['Tenant ID', 'SKU Name']
        missing_columns = [col for col in required_columns if col not in raw_usage_df.columns]
        
        if contract_col is None:
            missing_columns.append('Contract or SFDC#')
        
        if missing_columns:
            print(f"Error: Missing required columns in raw monthly usage file: {', '.join(missing_columns)}")
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Get unique combinations of Tenant ID, SKU Name, and Contract from raw usage file
        # Convert to string and handle any NaN values
        raw_usage_df['Tenant ID'] = raw_usage_df['Tenant ID'].astype(str)
        raw_usage_df['SKU Name'] = raw_usage_df['SKU Name'].astype(str)
        raw_usage_df['Contract'] = raw_usage_df[contract_col].astype(str)  # Normalize to 'Contract'
        
        # Create a set of tuples for matching (Tenant ID, SKU Name, Contract)
        matching_tuples = set(zip(raw_usage_df['Tenant ID'], raw_usage_df['SKU Name'], raw_usage_df['Contract']))
        
        # Make a copy of filtered_df to avoid modifying the original
        filtered_df_copy = filtered_df.copy()
        
        # Convert tenant_id, name, and contract_name to string for comparison
        filtered_df_copy['tenant_id'] = filtered_df_copy['tenant_id'].astype(str)
        filtered_df_copy['name'] = filtered_df_copy['name'].astype(str)
        filtered_df_copy['contract_name'] = filtered_df_copy['contract_name'].astype(str) if 'contract_name' in filtered_df_copy.columns else ''
        
        # Filter filtered_df to only include rows where (tenant_id, name, contract_name) matches
        # any combination in the raw usage file
        mask = filtered_df_copy.apply(
            lambda row: (str(row['tenant_id']), str(row['name']), str(row.get('contract_name', ''))) in matching_tuples,
            axis=1
        )
        
        # Create Tabs_bt_final_df with only matching rows
        Tabs_bt_clean_df = filtered_df_copy[mask].copy()
        
        print(f"Matched {len(Tabs_bt_clean_df)} rows using (Tenant ID, SKU Name, Contract) matching")
        
        return Tabs_bt_clean_df
        
    except Exception as e:
        print(f"Error processing raw monthly usage file: {str(e)}")
        return filtered_df


def enterprise_support(tabs_bt_clean_df, enterprise_support_file, billing_run_date=None):
    """
    Process enterprise support file and add additional rows to tabs_bt_clean_df.
    
    Args:
        tabs_bt_clean_df: Filtered DataFrame from tabs_billing_terms_to_upload
        enterprise_support_file: UploadedFile object from Streamlit file_uploader (CSV, XLSX, or XLS)
        billing_run_date: Optional billing run date in YYYY-MM-DD format to populate date columns
        
    Returns:
        pd.DataFrame: DataFrame with additional rows from enterprise support file
    """
    if enterprise_support_file is None:
        return tabs_bt_clean_df
    
    try:
        # Read the enterprise support file based on file extension
        file_extension = enterprise_support_file.name.split('.')[-1].lower()
        
        if file_extension == 'csv':
            enterprise_df = pd.read_csv(enterprise_support_file)
        elif file_extension in ['xlsx', 'xls']:
            enterprise_df = pd.read_excel(enterprise_support_file)
        else:
            print(f"Unsupported file type: {file_extension}")
            return tabs_bt_clean_df
        
        # Check if required columns exist in enterprise support file
        # Column E (index 4) is assumed to contain Enterprise Support %
        if 'Tenant ID' not in enterprise_df.columns:
            print(f"Error: Missing required column 'Tenant ID' in enterprise support file")
            print(f"Available columns: {', '.join(enterprise_df.columns.tolist())}")
            return tabs_bt_clean_df
        
        # Check if we have at least 5 columns (to access column E, index 4)
        if len(enterprise_df.columns) < 5:
            print(f"Error: Enterprise support file must have at least 5 columns (Column E for Enterprise Support %)")
            print(f"File has {len(enterprise_df.columns)} column(s)")
            return tabs_bt_clean_df
        
        # Use column E (index 4) for Enterprise Support %
        enterprise_support_col = enterprise_df.columns[4]
        enterprise_df['Enterprise Support %'] = enterprise_df[enterprise_support_col]
        
        # Get unique Tenant IDs from enterprise support file
        enterprise_df['Tenant ID'] = enterprise_df['Tenant ID'].astype(str)
        unique_tenant_ids = enterprise_df['Tenant ID'].unique()
        
        # Convert columns in tabs_bt_clean_df to string for matching
        tabs_bt_clean_df_copy = tabs_bt_clean_df.copy()
        tabs_bt_clean_df_copy['customer_id'] = tabs_bt_clean_df_copy['customer_id'].astype(str)
        tabs_bt_clean_df_copy['tenant_id'] = tabs_bt_clean_df_copy['tenant_id'].astype(str)
        
        # Create a mapping of Tenant ID (from enterprise file) to Tabs Customer ID
        # Tenant ID from enterprise file should match tenant_id column in tabs_bt_clean_df
        tenant_to_customer_id = {}
        for tenant_id in unique_tenant_ids:
            # Find matching rows where tenant_id column equals the Tenant ID from enterprise file
            matching_rows = tabs_bt_clean_df_copy[tabs_bt_clean_df_copy['tenant_id'] == tenant_id]
            if not matching_rows.empty:
                # Get the Tabs Customer ID (customer_id column) that corresponds to this tenant_id
                tabs_customer_id = matching_rows.iloc[0]['customer_id']
                tenant_to_customer_id[tenant_id] = tabs_customer_id
        
        if not tenant_to_customer_id:
            print(f"Warning: No matching Tabs Customer IDs found for Tenant IDs in enterprise support file")
            return tabs_bt_clean_df
        
        # Calculate revenue_end_date if billing_run_date is provided
        revenue_end_date = billing_run_date
        if billing_run_date:
            try:
                date_obj = datetime.strptime(billing_run_date, '%Y-%m-%d')
                end_date_obj = date_obj + timedelta(days=30)
                revenue_end_date = end_date_obj.strftime('%Y-%m-%d')
            except (ValueError, AttributeError):
                revenue_end_date = billing_run_date
        
        # Create new rows for each Tenant ID from enterprise support file
        new_rows = []
        for tenant_id_from_file, tabs_customer_id in tenant_to_customer_id.items():
            # Get the first row with this customer_id to copy some values
            matching_row = tabs_bt_clean_df_copy[tabs_bt_clean_df_copy['customer_id'] == tabs_customer_id].iloc[0]
            
            # Get Net Terms from matching row if available, otherwise use empty string
            net_terms_value = ''
            if 'Net Terms' in tabs_bt_clean_df_copy.columns:
                value = matching_row.get('Net Terms', '')
                if not pd.isna(value):
                    net_terms_value = str(value)
            if not net_terms_value and 'net_payment_terms' in tabs_bt_clean_df_copy.columns:
                value = matching_row.get('net_payment_terms', '')
                if not pd.isna(value):
                    net_terms_value = str(value)
            
            # Create a new row with hard-coded values
            new_row = {
                'customer_id': tabs_customer_id,  # Tabs Customer ID (from matching row)
                'tenant_id': tenant_id_from_file,  # Tenant ID from enterprise support file
                'contract_id': '',
                'invoice_date': billing_run_date,
                'is_recurring': "FALSE",
                'quantity': 1,
                'due_interval': 1,
                'due_interval_unit': 'MONTH',
                'duration': 1,
                'net_payment_terms': net_terms_value,
                'is_volume': 'FALSE',
                'billing_type': 'UNIT_PRICE',
                'invoiceDateStrategy': 'ARREARS',
                'event_to_track': 'a12f94a4-6634-4e98-9587-7700b42808ed',
                'name': 'Enterprise Support',
                'note': '',
                'integration_item_id': '',
                'revenue_start_date': billing_run_date,
                'revenue_end_date': revenue_end_date,
                'invoice_type': 'INVOICE',
                'revenue_product_id': '',
                'amount_1': '1',
                'value_1': '',
                'amount_2': '',
                'value_2': '',
                'class_id': ''
            }
            
            # Add any additional columns that might exist in tabs_bt_clean_df
            for col in tabs_bt_clean_df_copy.columns:
                if col not in new_row:
                    new_row[col] = matching_row.get(col, '')
            
            new_rows.append(new_row)
        
        # Create DataFrame from new rows
        if new_rows:
            new_rows_df = pd.DataFrame(new_rows)
            
            # Ensure column order matches tabs_bt_clean_df
            if tabs_bt_clean_df.columns.tolist():
                # Reorder columns to match tabs_bt_clean_df
                existing_columns = [col for col in tabs_bt_clean_df.columns if col in new_rows_df.columns]
                remaining_columns = [col for col in new_rows_df.columns if col not in tabs_bt_clean_df.columns]
                new_rows_df = new_rows_df[existing_columns + remaining_columns]
            
            # Append new rows to tabs_bt_clean_df
            tabs_bt_enterprise = pd.concat([tabs_bt_clean_df, new_rows_df], ignore_index=True)
            return tabs_bt_enterprise
        
        return tabs_bt_clean_df
        
    except Exception as e:
        print(f"Error processing enterprise support file: {str(e)}")
        return tabs_bt_clean_df


def prepaid(tabs_bt_enterprise, prepaid_file, billing_run_date=None):
    """
    Process prepaid file and add additional rows to tabs_bt_enterprise.
    
    Args:
        tabs_bt_enterprise: DataFrame from enterprise_support (or tabs_billing_terms_to_upload if enterprise_support not used)
        prepaid_file: UploadedFile object from Streamlit file_uploader (CSV, XLSX, or XLS)
        billing_run_date: Optional billing run date in YYYY-MM-DD format to populate date columns
        
    Returns:
        pd.DataFrame: DataFrame with additional rows from prepaid file
    """
    if prepaid_file is None:
        return tabs_bt_enterprise
    
    try:
        # Read the prepaid file based on file extension
        file_extension = prepaid_file.name.split('.')[-1].lower()
        
        if file_extension == 'csv':
            prepaid_df = pd.read_csv(prepaid_file)
        elif file_extension in ['xlsx', 'xls']:
            prepaid_df = pd.read_excel(prepaid_file)
        else:
            print(f"Unsupported file type: {file_extension}")
            return tabs_bt_enterprise
        
        # Check if prepaid file has at least 2 columns (Column A for Date Modified, Column B for Tenant ID)
        if len(prepaid_df.columns) < 2:
            print(f"Error: Prepaid file must have at least 2 columns (Column A: Date Modified, Column B: Tenant ID)")
            print(f"File has {len(prepaid_df.columns)} column(s)")
            return tabs_bt_enterprise
        
        # Use column B (index 1) for Tenant ID (Column A is Date Modified)
        tenant_id_col = prepaid_df.columns[1]
        # Convert to numeric first (handles floats), then to int, then to string to remove .0
        prepaid_df['Tenant ID'] = pd.to_numeric(prepaid_df[tenant_id_col], errors='coerce').fillna(0).astype(int).astype(str)
        
        # Get unique Tenant IDs from prepaid file
        unique_tenant_ids = prepaid_df['Tenant ID'].unique()
        
        # Convert columns in tabs_bt_enterprise to string for matching
        tabs_bt_enterprise_copy = tabs_bt_enterprise.copy()
        tabs_bt_enterprise_copy['customer_id'] = tabs_bt_enterprise_copy['customer_id'].astype(str)
        tabs_bt_enterprise_copy['tenant_id'] = tabs_bt_enterprise_copy['tenant_id'].astype(str)
        
        # Create a mapping of Tenant ID (from prepaid file) to Tabs Customer ID
        # Tenant ID from prepaid file should match tenant_id column in tabs_bt_enterprise
        tenant_to_customer_id = {}
        
        # Debug: Print what we're looking for and what's available
        print(f"Debug - Looking for Tenant IDs from prepaid file: {unique_tenant_ids}")
        if 'tenant_id' in tabs_bt_enterprise_copy.columns:
            available_tenant_ids = tabs_bt_enterprise_copy['tenant_id'].unique()
            print(f"Debug - Available tenant_ids in tabs_bt_enterprise: {available_tenant_ids}")
            print(f"Debug - tenant_id column type: {tabs_bt_enterprise_copy['tenant_id'].dtype}")
        else:
            print(f"Debug - ERROR: tenant_id column not found in tabs_bt_enterprise!")
            print(f"Debug - Available columns: {tabs_bt_enterprise_copy.columns.tolist()}")
        
        for tenant_id in unique_tenant_ids:
            # Find matching rows where tenant_id column equals the Tenant ID from prepaid file
            matching_rows = tabs_bt_enterprise_copy[tabs_bt_enterprise_copy['tenant_id'] == tenant_id]
            if not matching_rows.empty:
                # Get the Tabs Customer ID (customer_id column) that corresponds to this tenant_id
                tabs_customer_id = matching_rows.iloc[0]['customer_id']
                tenant_to_customer_id[tenant_id] = tabs_customer_id
                print(f"Debug - Found match: Tenant ID {tenant_id} -> Customer ID {tabs_customer_id}")
            else:
                print(f"Debug - No match found for Tenant ID: {tenant_id} (type: {type(tenant_id)})")
        
        if not tenant_to_customer_id:
            print(f"Warning: No matching Tabs Customer IDs found for Tenant IDs in prepaid file")
            return tabs_bt_enterprise
        
        # Calculate revenue_end_date if billing_run_date is provided
        revenue_end_date = billing_run_date
        if billing_run_date:
            try:
                date_obj = datetime.strptime(billing_run_date, '%Y-%m-%d')
                end_date_obj = date_obj + timedelta(days=30)
                revenue_end_date = end_date_obj.strftime('%Y-%m-%d')
            except (ValueError, AttributeError):
                revenue_end_date = billing_run_date
        
        # Create new rows for each Tenant ID from prepaid file
        new_rows = []
        for tenant_id_from_file, tabs_customer_id in tenant_to_customer_id.items():
            # Get the first row with this customer_id to copy some values
            matching_row = tabs_bt_enterprise_copy[tabs_bt_enterprise_copy['customer_id'] == tabs_customer_id].iloc[0]
            
            # Get Net Terms from matching row if available, otherwise use empty string
            net_terms_value = ''
            if 'Net Terms' in tabs_bt_enterprise_copy.columns:
                value = matching_row.get('Net Terms', '')
                if not pd.isna(value):
                    net_terms_value = str(value)
            if not net_terms_value and 'net_payment_terms' in tabs_bt_enterprise_copy.columns:
                value = matching_row.get('net_payment_terms', '')
                if not pd.isna(value):
                    net_terms_value = str(value)
            
            # Create a new row with hard-coded values
            new_row = {
                'customer_id': tabs_customer_id,  # Tabs Customer ID (from matching row)
                'tenant_id': tenant_id_from_file,  # Tenant ID from prepaid file
                'contract_id': '',
                'invoice_date': billing_run_date,
                'is_recurring': "TRUE",
                'quantity': 1,
                'due_interval': 1,
                'due_interval_unit': 'MONTH',
                'duration': 1,
                'net_payment_terms': net_terms_value,
                'is_volume': 'FALSE',
                'billing_type': 'UNIT_PRICE',
                'invoiceDateStrategy': 'ARREARS',
                'event_to_track': '48ef8004-9735-4830-99fc-801161eb8d7f',
                'name': 'Prepaid',  
                'note': '',
                'integration_item_id': '',
                'revenue_start_date': billing_run_date,
                'revenue_end_date': revenue_end_date,
                'invoice_type': 'INVOICE',
                'revenue_product_id': '',
                'amount_1': '-1',  
                'value_1': '',
                'amount_2': '',
                'value_2': '',
                'class_id': ''
            }
            
            # Add any additional columns that might exist in tabs_bt_enterprise
            for col in tabs_bt_enterprise_copy.columns:
                if col not in new_row:
                    new_row[col] = matching_row.get(col, '')
            
            new_rows.append(new_row)
        
        # Create DataFrame from new rows
        if new_rows:
            new_rows_df = pd.DataFrame(new_rows)
            
            # Ensure column order matches tabs_bt_enterprise
            if tabs_bt_enterprise.columns.tolist():
                # Reorder columns to match tabs_bt_enterprise
                existing_columns = [col for col in tabs_bt_enterprise.columns if col in new_rows_df.columns]
                remaining_columns = [col for col in new_rows_df.columns if col not in tabs_bt_enterprise.columns]
                new_rows_df = new_rows_df[existing_columns + remaining_columns]
            
            # Append new rows to tabs_bt_enterprise
            tabs_bt_prepaid_enterprise = pd.concat([tabs_bt_enterprise, new_rows_df], ignore_index=True)
            return tabs_bt_prepaid_enterprise
        
        return tabs_bt_enterprise
        
    except Exception as e:
        print(f"Error processing prepaid file: {str(e)}")
        return tabs_bt_enterprise


def create_contracts(tabs_bt_prepaid_enterprise):
    """
    Create contracts using all unique customer_id values from the DataFrame.
    Calls the Tabs API to create contracts for each unique customer and updates
    the contract_id column in tabs_bt_prepaid_enterprise for all rows.
    
    Args:
        tabs_bt_prepaid_enterprise: DataFrame from prepaid function (or tabs_bt_enterprise if prepaid not used)
        
    Returns:
        pd.DataFrame: tabs_bt_contract with contract_id populated for all rows
    """
    if tabs_bt_prepaid_enterprise is None or tabs_bt_prepaid_enterprise.empty:
        return tabs_bt_prepaid_enterprise
    
    try:
        # Make a copy to avoid modifying the original
        tabs_bt_contract = tabs_bt_prepaid_enterprise.copy()
        
        # Get all unique customer_id values
        unique_customer_ids = tabs_bt_contract['customer_id'].unique()
        
        # Convert to string and remove any NaN values
        unique_customer_ids = [str(cid) for cid in unique_customer_ids if pd.notna(cid)]
        
        if not unique_customer_ids:
            print("Warning: No valid customer_ids found in DataFrame")
            return tabs_bt_contract
        
        # Create a mapping of customer_id -> contract_id
        customer_to_contract_id = {}
        
        # For each unique customer_id, create a contract via API
        for customer_id in unique_customer_ids:
            # Find the first row with this customer_id to get reference data
            matching_rows = tabs_bt_contract[tabs_bt_contract['customer_id'] == customer_id]
            if not matching_rows.empty:
                reference_row = matching_rows.iloc[0]
                tenant_id = reference_row.get('tenant_id', '')
                invoice_date = reference_row.get('invoice_date', '')
                
                # Generate contract name using tenant_id + invoice_date
                if tenant_id and invoice_date:
                    contract_name = f"{tenant_id}_{invoice_date}"
                elif tenant_id:
                    contract_name = f"{tenant_id}_"
                elif invoice_date:
                    contract_name = f"_{invoice_date}"
                else:
                    contract_name = f"Contract for Customer {customer_id}"
                
                # Call API to create contract
                contract_id = None
                try:
                    result = create_contract(customer_id, contract_name)
                    if result:
                        contract_id, full_payload = result
                        print(f"Successfully created contract {contract_id} for customer {customer_id}")
                        customer_to_contract_id[customer_id] = contract_id
                    else:
                        print(f"Warning: Failed to create contract for customer {customer_id}")
                except Exception as api_error:
                    print(f"Error creating contract for customer {customer_id}: {str(api_error)}")
        
        # Update contract_id column in the DataFrame for all rows based on customer_id
        if customer_to_contract_id:
            # Ensure contract_id column exists
            if 'contract_id' not in tabs_bt_contract.columns:
                tabs_bt_contract['contract_id'] = ''
            
            # Store original customer_id type
            original_customer_id_type = tabs_bt_contract['customer_id'].dtype
            
            # Convert customer_id to string for matching
            tabs_bt_contract['customer_id'] = tabs_bt_contract['customer_id'].astype(str)
            
            # Map contract_id based on customer_id
            # Use the mapped value if available, otherwise keep existing value
            tabs_bt_contract['contract_id'] = tabs_bt_contract['customer_id'].map(customer_to_contract_id).fillna(tabs_bt_contract['contract_id'])
            
            # Fill any remaining NaN values with empty string
            tabs_bt_contract['contract_id'] = tabs_bt_contract['contract_id'].fillna('')
            
            # Convert customer_id back to original type if it was numeric
            if pd.api.types.is_numeric_dtype(original_customer_id_type):
                try:
                    tabs_bt_contract['customer_id'] = tabs_bt_contract['customer_id'].astype(original_customer_id_type)
                except (ValueError, TypeError):
                    # If conversion fails, keep as string
                    pass
        
        return tabs_bt_contract
        
    except Exception as e:
        print(f"Error creating contracts: {str(e)}")
        return tabs_bt_prepaid_enterprise

def create_invoices(tabs_bt_contract):
    """
    Push billing terms from tabs_bt_contract to Tabs API using push_bt function.
    Converts DataFrame to CSV format and uploads via bulk-create-billing-schedules endpoint as multipart/form-data.
    
    Args:
        tabs_bt_contract: DataFrame with contract_id and billing term information
        
    Returns:
        pd.DataFrame: Original DataFrame with push status information added
    """
    if tabs_bt_contract is None or tabs_bt_contract.empty:
        return tabs_bt_contract
    
    try:
        # Make a copy to avoid modifying the original
        result_df = tabs_bt_contract.copy()
        
        # Debug: Log what's being sent to push_bt
        print(f"\n=== DEBUG: create_invoices ===")
        print(f"Rows in tabs_bt_contract being sent to push_bt: {len(result_df)}")
        if 'customer_id' in result_df.columns:
            unique_customers = result_df['customer_id'].nunique()
            print(f"Unique customer_ids: {unique_customers}")
        if 'name' in result_df.columns:
            unique_names = result_df['name'].nunique()
            print(f"Unique SKU names: {unique_names}")
        print("=" * 40 + "\n")
        
        # Remove tenant_id column if it exists
        if 'tenant_id' in result_df.columns:
            result_df = result_df.drop(columns=['tenant_id'])
        
        # Add columns to track push status
        result_df['push_status'] = ''
        result_df['push_error'] = ''
        result_df['billing_term_id'] = ''
        
        # Convert DataFrame to CSV format in memory
        csv_buffer = io.StringIO()
        result_df.to_csv(csv_buffer, index=False)
        csv_string = csv_buffer.getvalue()
        
        # Debug: Print columns and sample data to verify event_to_track is included
        print(f"  Debug - Columns being sent to API: {list(result_df.columns)}")
        if 'event_to_track' in result_df.columns:
            non_null_count = result_df['event_to_track'].notna().sum()
            print(f"  Debug - event_to_track column exists with {non_null_count} non-null values out of {len(result_df)} total rows")
            sample_values = result_df['event_to_track'].head(3).tolist()
            print(f"  Debug - Sample event_to_track values: {sample_values}")
        else:
            print(f"  ⚠ Warning - event_to_track column NOT found in DataFrame!")
        
        # Prepare CSV file data for multipart/form-data upload
        # Format: (filename, file_data, content_type)
        csv_file_data = ('billing_schedules.csv', csv_string.encode('utf-8'), 'text/csv')
        
        # Call push_bt API function with CSV file data
        response = push_bt(csv_file_data)
        
        # Handle response
        if response and hasattr(response, 'status_code'):
            if response.status_code == 201:
                # Success - extract billingTermIds from response
                try:
                    response_data = response.json()
                    # Debug: Print full response
                    print(f"  Debug - Full response in create_invoices: {response_data}")
                    
                    # Check if billingTermIds is nested under payload or data
                    billing_term_ids = response_data.get('billingTermIds', [])
                    if not billing_term_ids and 'payload' in response_data:
                        payload = response_data.get('payload', {})
                        billing_term_ids = payload.get('billingTermIds', [])
                    if not billing_term_ids and 'data' in response_data:
                        data = response_data.get('data', {})
                        billing_term_ids = data.get('billingTermIds', [])
                    
                    # Update push status for all rows
                    result_df['push_status'] = 'SUCCESS'
                    
                    # If we have billing term IDs, try to map them to rows
                    # Note: The API returns IDs but may not match row order
                    if billing_term_ids and len(billing_term_ids) == len(result_df):
                        result_df['billing_term_id'] = billing_term_ids
                    elif billing_term_ids:
                        # If count doesn't match, just store the first N
                        result_df.loc[:len(billing_term_ids)-1, 'billing_term_id'] = billing_term_ids[:len(result_df)]
                    
                    print(f"✓ push_bt: Successfully processed {len(billing_term_ids)} billing term(s) in create_invoices")
                except Exception as parse_error:
                    result_df['push_status'] = 'SUCCESS'
                    result_df['push_error'] = f"Response parsed but no billingTermIds: {str(parse_error)}"
                    print(f"⚠ push_bt: API call succeeded but could not parse billingTermIds from response: {str(parse_error)}")
            else:
                # Failed - extract error message
                error_msg = f"HTTP {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', error_data.get('error', error_msg))
                except:
                    error_msg = response.text if hasattr(response, 'text') else error_msg
                
                result_df['push_status'] = 'FAILED'
                result_df['push_error'] = error_msg
                print(f"✗ push_bt: Failed to push billing terms in create_invoices - {error_msg}")
        else:
            result_df['push_status'] = 'FAILED'
            result_df['push_error'] = 'Invalid response from API'
            print(f"✗ push_bt: Failed to push billing terms in create_invoices - Invalid response from API")
        
        return result_df
        
    except Exception as e:
        print(f"Error in create_invoices: {str(e)}")
        return tabs_bt_contract


def create_tabs_ready_usage(raw_monthly_usage_file, tabs_bt_contract, enterprise_support_file=None, billing_run_date=None):
    """
    Generate CSV-ready DataFrame from tabs_bt_contract.
    Creates one usage row for each billing term in tabs_bt_contract.
    Looks up Meter values from raw monthly usage file based on (customer_id, SKU Name) combinations.
    Adds Enterprise Support and Prepaid rows with calculated values.
    
    Args:
        raw_monthly_usage_file: UploadedFile object from Streamlit file_uploader (CSV, XLSX, or XLS)
        tabs_bt_contract: DataFrame with contract information containing customer_id and name columns
        enterprise_support_file: Optional UploadedFile object from Streamlit file_uploader (CSV, XLSX, or XLS) with Enterprise Support % data
        billing_run_date: Optional billing run date in YYYY-MM-DD format (defaults to date two weeks ago if not provided)
        
    Returns:
        pd.DataFrame: DataFrame with columns: customer_id, event_type_id, event_type_name, datetime, value, differentiator, invoice
    """
    if raw_monthly_usage_file is None:
        return pd.DataFrame()
    
    if tabs_bt_contract is None or tabs_bt_contract.empty:
        print("Warning: tabs_bt_contract is empty or None, cannot create usage rows")
        return pd.DataFrame()
    
    # Debug: Log initial tabs_bt_contract info
    print(f"\n=== DEBUG: create_tabs_ready_usage ===")
    print(f"Initial tabs_bt_contract rows: {len(tabs_bt_contract)}")
    if 'customer_id' in tabs_bt_contract.columns:
        unique_customer_ids_bt = tabs_bt_contract['customer_id'].nunique()
        print(f"Unique customer_ids in tabs_bt_contract: {unique_customer_ids_bt}")
        if 'name' in tabs_bt_contract.columns:
            unique_names_bt = tabs_bt_contract['name'].nunique()
            print(f"Unique SKU names in tabs_bt_contract: {unique_names_bt}")
    
    try:
        # Read the raw monthly usage file to create lookup dictionary
        file_extension = raw_monthly_usage_file.name.split('.')[-1].lower()
        
        if file_extension == 'csv':
            raw_usage_df = pd.read_csv(raw_monthly_usage_file)
        elif file_extension in ['xlsx', 'xls']:
            raw_usage_df = pd.read_excel(raw_monthly_usage_file)
        else:
            print(f"Unsupported file type: {file_extension}")
            return pd.DataFrame()
        
        # Check if required columns exist in raw usage file (including Contract/SFDC# and Tenant Name)
        # Accept either 'Contract' or 'SFDC#' as the contract column name
        contract_col = 'Contract' if 'Contract' in raw_usage_df.columns else ('SFDC#' if 'SFDC#' in raw_usage_df.columns else None)
        
        required_columns = ['Tenant ID', 'Tenant Name', 'SKU Name', 'Meter']
        missing_columns = [col for col in required_columns if col not in raw_usage_df.columns]
        
        if contract_col is None:
            missing_columns.append('Contract or SFDC#')
        
        if missing_columns:
            print(f"Error: Missing required columns in raw monthly usage file: {', '.join(missing_columns)}")
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Convert columns to string for matching
        raw_usage_df['Tenant ID'] = raw_usage_df['Tenant ID'].astype(str)
        raw_usage_df['Tenant Name'] = raw_usage_df['Tenant Name'].astype(str)
        raw_usage_df['Contract'] = raw_usage_df[contract_col].astype(str)  # Normalize to 'Contract'
        
        # Get all customers from API to map tenant_id to Tabs Customer ID
        try:
            customers_data = get_all_customers()
            
            # Create mapping dictionary: tenant_id -> tabs_customer_id
            tenant_to_customer_id = {}
            for customer in customers_data:
                tabs_customer_id = customer.get('id')
                custom_fields = customer.get('customFields', [])
                
                # Find the Tenant ID custom field, fallback to Account # if not found
                tenant_id_value = None
                for field in custom_fields:
                    field_name = field.get('customFieldName')
                    if field_name == 'Tenant ID':
                        tenant_id_value = field.get('customFieldValue')
                        if tenant_id_value:
                            tenant_to_customer_id[str(tenant_id_value)] = tabs_customer_id
                            break
                
                # If Tenant ID not found, try Account #
                if not tenant_id_value:
                    for field in custom_fields:
                        if field.get('customFieldName') == 'Account #':
                            tenant_id_value = field.get('customFieldValue')
                            if tenant_id_value:
                                tenant_to_customer_id[str(tenant_id_value)] = tabs_customer_id
                                break
            
            # Map Tenant ID to customer_id in raw usage
            raw_usage_df['customer_id'] = raw_usage_df['Tenant ID'].map(tenant_to_customer_id)
            
            # Check for unmatched tenant IDs
            unmatched_rows = raw_usage_df[raw_usage_df['customer_id'].isna()]
            if not unmatched_rows.empty:
                unmatched_tenant_ids = unmatched_rows['Tenant ID'].unique()
                print(f"Warning: No matching Tabs Customer ID found for tenant ID(s): {', '.join(sorted(unmatched_tenant_ids, key=str))}")
            
            # Filter out rows where customer_id mapping failed
            raw_usage_df = raw_usage_df[raw_usage_df['customer_id'].notna()].copy()
            
            # Create a lookup dictionary: (customer_id, SKU Name, Contract, Tenant Name) -> Meter value (sum if multiple rows)
            # This groups by Tenant Name so different Tenant Names keep separate rows
            usage_lookup = {}
            for _, row in raw_usage_df.iterrows():
                customer_id = str(row['customer_id'])
                sku_name = str(row['SKU Name'])
                contract = str(row['Contract'])
                tenant_name = str(row['Tenant Name'])
                meter_value = row['Meter']
                
                key = (customer_id, sku_name, contract, tenant_name)
                if key not in usage_lookup:
                    usage_lookup[key] = 0
                
                # Sum if multiple rows exist for same combination (same Tenant ID + Tenant Name + SKU)
                try:
                    meter_float = float(str(meter_value).replace(',', '').strip()) if pd.notna(meter_value) else 0
                    usage_lookup[key] += meter_float
                except (ValueError, TypeError):
                    pass
            
            print(f"Created usage lookup with {len(usage_lookup)} (customer_id, SKU, Contract, Tenant Name) combinations")
            
            # Create invoice number mapping: For each Tenant ID, assign sequential numbers to unique Tenant Names
            # invoice_mapping: (Tenant ID, Tenant Name) -> invoice number
            tenant_id_to_tenant_names = {}
            for _, row in raw_usage_df.iterrows():
                tenant_id = str(row['Tenant ID'])
                tenant_name = str(row['Tenant Name'])
                if tenant_id not in tenant_id_to_tenant_names:
                    tenant_id_to_tenant_names[tenant_id] = []
                if tenant_name not in tenant_id_to_tenant_names[tenant_id]:
                    tenant_id_to_tenant_names[tenant_id].append(tenant_name)
            
            # Create the invoice mapping
            invoice_mapping = {}
            for tenant_id, tenant_names in tenant_id_to_tenant_names.items():
                for idx, tenant_name in enumerate(tenant_names, start=1):
                    invoice_mapping[(tenant_id, tenant_name)] = idx
            
            print(f"Created invoice mapping for {len(invoice_mapping)} (Tenant ID, Tenant Name) combinations")
            
        except Exception as e:
            print(f"Error fetching customers from API: {str(e)}")
            return pd.DataFrame()
        
        # Use billing_run_date if provided, otherwise use date two weeks ago
        if billing_run_date:
            try:
                datetime.strptime(billing_run_date, '%Y-%m-%d')
                billing_date = billing_run_date
            except ValueError:
                print(f"Warning: Invalid billing_run_date format. Expected YYYY-MM-DD, got: {billing_run_date}")
                billing_date = (datetime.now() - timedelta(weeks=2)).strftime('%Y-%m-%d')
        else:
            billing_date = (datetime.now() - timedelta(weeks=2)).strftime('%Y-%m-%d')
        
        # Create reverse mapping: customer_id -> tenant_id
        customer_id_to_tenant_id = {v: k for k, v in tenant_to_customer_id.items()}
        
        # Get set of valid (customer_id, sku_name, contract_name) from tabs_bt_contract for filtering
        # Also create mapping for event_type_id lookup
        valid_billing_terms = set()
        event_type_id_map = {}  # (customer_id, sku_name, contract_name) -> event_to_track
        enterprise_support_rows = []
        prepaid_rows = []
        
        for _, bt_row in tabs_bt_contract.iterrows():
            customer_id = str(bt_row.get('customer_id', ''))
            sku_name = str(bt_row.get('name', ''))
            contract_name = str(bt_row.get('contract_name', '')) if 'contract_name' in bt_row.index else ''
            event_to_track = str(bt_row.get('event_to_track', '')) if pd.notna(bt_row.get('event_to_track')) else ''
            
            if not customer_id or not sku_name or customer_id == 'nan' or sku_name == 'nan':
                continue
            
            # Store event_type_id mapping for all billing terms
            event_type_id_map[(customer_id, sku_name, contract_name)] = event_to_track
            
            # Check if this is Enterprise Support or Prepaid row
            is_enterprise_support = 'Enterprise Support' in sku_name or sku_name == 'Enterprise Support'
            is_prepaid = 'Prepaid' in sku_name or sku_name == 'Prepaid'
            
            if is_enterprise_support:
                # Store for later processing (needs calculation based on other rows)
                enterprise_support_rows.append({
                    'customer_id': customer_id,
                    'name': sku_name,
                    'contract_name': contract_name,
                    'event_type_id': event_to_track,
                    'bt_row': bt_row
                })
            elif is_prepaid:
                # Store for later processing (needs calculation based on other rows)
                prepaid_rows.append({
                    'customer_id': customer_id,
                    'name': sku_name,
                    'contract_name': contract_name,
                    'event_type_id': event_to_track,
                    'bt_row': bt_row
                })
            else:
                # Add to valid billing terms set
                valid_billing_terms.add((customer_id, sku_name, contract_name))
        
        # Create output_df by iterating through usage_lookup
        # This creates one row per unique (customer_id, SKU, Contract, Tenant Name) combination
        output_rows = []
        for lookup_key, meter_value in usage_lookup.items():
            customer_id, sku_name, contract, tenant_name = lookup_key
            
            # Only include rows that match a billing term in tabs_bt_contract
            if (customer_id, sku_name, contract) not in valid_billing_terms:
                continue
            
            # Get tenant_id from customer_id for invoice lookup
            tenant_id = customer_id_to_tenant_id.get(customer_id, '')
            
            # Get invoice number from mapping
            invoice_num = invoice_mapping.get((tenant_id, tenant_name), 1)
            
            # Get event_type_id from mapping
            event_type_id = event_type_id_map.get((customer_id, sku_name, contract), '')
            
            output_rows.append({
                'customer_id': customer_id,
                'event_type_id': event_type_id,
                'event_type_name': sku_name,
                'datetime': billing_date,
                'value': meter_value,
                'differentiator': '',
                'invoice': invoice_num
            })
        
        # Create initial output_df from regular billing terms
        output_df = pd.DataFrame(output_rows)
        print(f"Created {len(output_df)} usage rows from regular billing terms")
        
        # Create mapping from (customer_id, contract) -> invoice number for Enterprise Support/Prepaid rows
        # This allows us to assign the correct invoice number based on contract
        contract_to_invoice = {}
        for row_data in output_rows:
            customer_id = row_data.get('customer_id', '')
            invoice_num = row_data.get('invoice', '')
            # We need to get the contract from the usage_lookup key
            # The contract is stored in the lookup key, let's rebuild this mapping
        
        # Rebuild contract mapping from usage_lookup keys
        for lookup_key, _ in usage_lookup.items():
            customer_id, sku_name, contract, tenant_name = lookup_key
            if (customer_id, sku_name, contract) in valid_billing_terms:
                tenant_id = customer_id_to_tenant_id.get(customer_id, '')
                invoice_num = invoice_mapping.get((tenant_id, tenant_name), 1)
                # Store (customer_id, contract) -> invoice_num
                # If multiple tenant names exist for same contract, just use the first one found
                if (customer_id, contract) not in contract_to_invoice:
                    contract_to_invoice[(customer_id, contract)] = invoice_num
        
        print(f"Created contract_to_invoice mapping with {len(contract_to_invoice)} entries")
        
        # Process Enterprise Support rows (if any)
        if enterprise_support_rows:
            # Read Enterprise Support % from enterprise_support_file if provided
            customer_to_enterprise_pct = {}
            if enterprise_support_file is not None:
                try:
                    # Read the enterprise support file based on file extension
                    file_extension = enterprise_support_file.name.split('.')[-1].lower()
                
                    if file_extension == 'csv':
                        enterprise_df = pd.read_csv(enterprise_support_file)
                    elif file_extension in ['xlsx', 'xls']:
                        enterprise_df = pd.read_excel(enterprise_support_file)
                    else:
                        print(f"Unsupported file type for enterprise support file: {file_extension}")
                        enterprise_df = pd.DataFrame()
                    
                    if not enterprise_df.empty:
                        # Check if required columns exist
                        # Column E (index 4) is assumed to contain Enterprise Support %
                        if 'Tenant ID' not in enterprise_df.columns:
                            print(f"Warning: Missing required column 'Tenant ID' in enterprise support file")
                            print(f"Available columns: {', '.join(enterprise_df.columns.tolist())}")
                        elif len(enterprise_df.columns) < 5:
                            print(f"Warning: Enterprise support file must have at least 5 columns (Column E for Enterprise Support %)")
                            print(f"File has {len(enterprise_df.columns)} column(s)")
                        else:
                            # Use column E (index 4) for Enterprise Support %
                            enterprise_support_col = enterprise_df.columns[4]
                            enterprise_df['Enterprise Support %'] = enterprise_df[enterprise_support_col]
                            
                            # Convert Tenant ID to string
                            enterprise_df['Tenant ID'] = enterprise_df['Tenant ID'].astype(str)
                            
                            # Get all customers from API to map Tenant ID to customer_id
                            try:
                                customers_data = get_all_customers()
                                
                                # Create mapping: tenant_id -> tabs_customer_id
                                tenant_to_customer_id = {}
                                for customer in customers_data:
                                    tabs_customer_id = customer.get('id')
                                    custom_fields = customer.get('customFields', [])
                                    
                                    # Find the Tenant ID custom field, fallback to Account # if not found
                                    tenant_id_value = None
                                    for field in custom_fields:
                                        field_name = field.get('customFieldName')
                                        if field_name == 'Tenant ID':
                                            tenant_id_value = field.get('customFieldValue')
                                            if tenant_id_value:
                                                tenant_to_customer_id[str(tenant_id_value)] = tabs_customer_id
                                                break
                                    
                                    # If Tenant ID not found, try Account #
                                    if not tenant_id_value:
                                        for field in custom_fields:
                                            if field.get('customFieldName') == 'Account #':
                                                tenant_id_value = field.get('customFieldValue')
                                                if tenant_id_value:
                                                    tenant_to_customer_id[str(tenant_id_value)] = tabs_customer_id
                                                    break
                                
                                # Create mapping: customer_id -> Enterprise Support %
                                for _, row in enterprise_df.iterrows():
                                    tenant_id = str(row['Tenant ID'])
                                    enterprise_pct = row['Enterprise Support %']
                                    
                                    # Convert percentage to decimal if needed (handle both "50" and "50%" formats)
                                    if pd.notna(enterprise_pct):
                                        try:
                                            # Try to convert to float
                                            pct_value = float(str(enterprise_pct).replace('%', '').strip())
                                            # If value is > 1, assume it's a percentage and divide by 100
                                            if pct_value > 1:
                                                pct_value = pct_value / 100
                                            # Map Tenant ID to customer_id
                                            if tenant_id in tenant_to_customer_id:
                                                customer_id = tenant_to_customer_id[tenant_id]
                                                customer_to_enterprise_pct[str(customer_id)] = pct_value
                                        except (ValueError, TypeError):
                                            print(f"Warning: Could not parse Enterprise Support % for Tenant ID {tenant_id}: {enterprise_pct}")
                            
                            except Exception as e:
                                print(f"Error fetching customers for Enterprise Support mapping: {str(e)}")
                
                except Exception as e:
                    print(f"Error reading enterprise support file: {str(e)}")
            
            # Process Enterprise Support rows
            enterprise_new_rows = []
            for es_info in enterprise_support_rows:
                customer_id = es_info['customer_id']
                sku_name = es_info['name']
                
                # Calculate value using formula: sum(value * amount_1) * Enterprise Support %
                calculated_value = 0
                
                # Get Enterprise Support % for this customer_id
                enterprise_pct = customer_to_enterprise_pct.get(customer_id, 0)
                
                if enterprise_pct > 0:
                    # Find all rows in output_df with this customer_id (excluding Enterprise Support rows)
                    customer_rows = output_df[(output_df['customer_id'] == customer_id) & 
                                              (output_df['event_type_name'] != 'Enterprise Support')]
                    
                    if not customer_rows.empty:
                        # Match each row to tabs_bt_contract to get amount_1
                        # Use Decimal for precise financial calculations
                        sum_product = Decimal('0')
                        for _, row in customer_rows.iterrows():
                            event_type_name = row['event_type_name']
                            value = row['value']
                            
                            # Try to convert value to Decimal
                            try:
                                value_str = str(value).replace(',', '').strip() if pd.notna(value) and str(value).strip() else '0'
                                value_decimal = Decimal(value_str)
                            except:
                                value_decimal = Decimal('0')
                            
                            # Find matching row in tabs_bt_contract by customer_id and name/event_type_name
                            matching_contract_rows = tabs_bt_contract[
                                (tabs_bt_contract['customer_id'].astype(str) == customer_id) &
                                (tabs_bt_contract['name'].astype(str) == str(event_type_name))
                            ]
                            
                            if not matching_contract_rows.empty:
                                # Get amount_1 from first matching row
                                amount_1 = matching_contract_rows.iloc[0].get('amount_1', 0)
                                try:
                                    amount_1_str = str(amount_1).replace(',', '').strip() if pd.notna(amount_1) and str(amount_1).strip() else '0'
                                    amount_1_decimal = Decimal(amount_1_str)
                                except:
                                    amount_1_decimal = Decimal('0')
                                
                                # Calculate value * amount_1 using Decimal
                                sum_product += value_decimal * amount_1_decimal
                        
                        # Multiply sum by Enterprise Support % and round with ROUND_HALF_UP
                        enterprise_pct_decimal = Decimal(str(enterprise_pct))
                        calculated_value = float((sum_product * enterprise_pct_decimal).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                
                # Get invoice number from contract mapping
                contract_name = es_info.get('contract_name', '')
                invoice_num = contract_to_invoice.get((customer_id, contract_name), 1)
                
                # Get event_type_id from stored value
                event_type_id = es_info.get('event_type_id', '')
                
                enterprise_new_rows.append({
                    'customer_id': customer_id,
                    'event_type_id': event_type_id,
                    'event_type_name': sku_name,
                    'datetime': billing_date,
                    'value': calculated_value,
                    'differentiator': '',
                    'invoice': invoice_num
                })
            
            # Append Enterprise Support rows to output_df
            if enterprise_new_rows:
                enterprise_df = pd.DataFrame(enterprise_new_rows)
                output_df = pd.concat([output_df, enterprise_df], ignore_index=True)
                print(f"Added {len(enterprise_new_rows)} Enterprise Support row(s) to output_df")
        
        # Reset index after adding Enterprise Support rows
        output_df = output_df.reset_index(drop=True)
        
        # Process Prepaid rows (if any)
        if prepaid_rows:
            prepaid_new_rows = []
            for prepaid_info in prepaid_rows:
                customer_id = prepaid_info['customer_id']
                sku_name = prepaid_info['name']
                
                # Calculate value using formula: sum(value * amount_1) for all customer rows (including Enterprise Support)
                calculated_value = 0
                
                # Find all rows in output_df with this customer_id (INCLUDING Enterprise Support rows)
                customer_rows = output_df[output_df['customer_id'] == customer_id]
                
                if not customer_rows.empty:
                    # Match each row to tabs_bt_contract to get amount_1
                    # Use Decimal for precise financial calculations
                    sum_product = Decimal('0')
                    for _, row in customer_rows.iterrows():
                        event_type_name = row['event_type_name']
                        value = row['value']
                        
                        # Try to convert value to Decimal
                        try:
                            value_str = str(value).replace(',', '').strip() if pd.notna(value) and str(value).strip() else '0'
                            value_decimal = Decimal(value_str)
                        except:
                            value_decimal = Decimal('0')
                        
                        # Find matching row in tabs_bt_contract by customer_id and name/event_type_name
                        matching_contract_rows = tabs_bt_contract[
                            (tabs_bt_contract['customer_id'].astype(str) == customer_id) &
                            (tabs_bt_contract['name'].astype(str) == str(event_type_name))
                        ]
                        
                        if not matching_contract_rows.empty:
                            # Get amount_1 from first matching row
                            amount_1 = matching_contract_rows.iloc[0].get('amount_1', 0)
                            try:
                                amount_1_str = str(amount_1).replace(',', '').strip() if pd.notna(amount_1) and str(amount_1).strip() else '0'
                                amount_1_decimal = Decimal(amount_1_str)
                            except:
                                amount_1_decimal = Decimal('0')
                            
                            # Calculate value * amount_1 using Decimal
                            sum_product += value_decimal * amount_1_decimal
                    
                    # Set calculated value with ROUND_HALF_UP to 2 decimal places
                    calculated_value = float(sum_product.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                
                # Get invoice number from contract mapping
                contract_name = prepaid_info.get('contract_name', '')
                invoice_num = contract_to_invoice.get((customer_id, contract_name), 1)
                
                # Get event_type_id from stored value
                event_type_id = prepaid_info.get('event_type_id', '')
                
                prepaid_new_rows.append({
                    'customer_id': customer_id,
                    'event_type_id': event_type_id,
                    'event_type_name': sku_name,
                    'datetime': billing_date,
                    'value': calculated_value,
                    'differentiator': '',
                    'invoice': invoice_num
                })
            
            # Append Prepaid rows to output_df
            if prepaid_new_rows:
                prepaid_df = pd.DataFrame(prepaid_new_rows)
                output_df = pd.concat([output_df, prepaid_df], ignore_index=True)
                print(f"Added {len(prepaid_new_rows)} Prepaid row(s) to output_df")
        
        # Reset index after adding Prepaid rows
        output_df = output_df.reset_index(drop=True)
        
        # Debug: Final summary
        print(f"\n=== DEBUG: Final Summary ===")
        print(f"Final usage output rows: {len(output_df)}")
        print(f"tabs_bt_contract rows: {len(tabs_bt_contract)}")
        difference = len(tabs_bt_contract) - len(output_df)
        print(f"Difference: {difference} rows {'✓ MATCH' if difference == 0 else '✗ MISMATCH'}")
        if len(output_df) > 0 and 'customer_id' in output_df.columns:
            final_unique_customers = output_df['customer_id'].nunique()
            print(f"Unique customer_ids in final output: {final_unique_customers}")
        if len(output_df) > 0 and 'event_type_name' in output_df.columns:
            final_unique_skus = output_df['event_type_name'].nunique()
            print(f"Unique SKU names in final output: {final_unique_skus}")
        print("=" * 40 + "\n")
        
        print(f"Successfully created usage DataFrame with {len(output_df)} rows")
        return output_df
        
    except Exception as e:
        print(f"Error processing raw monthly usage file: {str(e)}")
        return pd.DataFrame()


def generate_prepaid_report_data(usage_df: pd.DataFrame, tabs_bt_contract: pd.DataFrame) -> dict:
    """
    Generate prepaid report data for updating Google Sheet.
    Returns a dictionary mapping tenant_id to calculated prepaid value.
    
    Args:
        usage_df: The usage DataFrame from create_tabs_ready_usage
        tabs_bt_contract: The billing terms contract DataFrame
        
    Returns:
        dict: {tenant_id: calculated_prepaid_value}
    """
    prepaid_values = {}
    
    if usage_df is None or usage_df.empty:
        return prepaid_values
    
    if tabs_bt_contract is None or tabs_bt_contract.empty:
        return prepaid_values
    
    # Find Prepaid rows in usage_df
    prepaid_rows = usage_df[usage_df['event_type_name'].str.contains('Prepaid', case=False, na=False)]
    
    if prepaid_rows.empty:
        return prepaid_values
    
    # Get tenant_id for each customer_id
    # Create reverse mapping from customer_id to tenant_id
    customer_to_tenant = {}
    if 'customer_id' in tabs_bt_contract.columns and 'tenant_id' in tabs_bt_contract.columns:
        for _, row in tabs_bt_contract.iterrows():
            customer_id = str(row.get('customer_id', ''))
            tenant_id = str(row.get('tenant_id', ''))
            if customer_id and tenant_id and customer_id != 'nan':
                customer_to_tenant[customer_id] = tenant_id
    
    # Extract prepaid values by tenant_id
    for _, row in prepaid_rows.iterrows():
        customer_id = str(row.get('customer_id', ''))
        value = row.get('value', 0)
        
        if customer_id in customer_to_tenant:
            tenant_id = customer_to_tenant[customer_id]
            
            try:
                value_float = float(str(value).replace(',', '').strip()) if pd.notna(value) else 0
            except (ValueError, TypeError):
                value_float = 0
            
            if tenant_id in prepaid_values:
                prepaid_values[tenant_id] += value_float
            else:
                prepaid_values[tenant_id] = value_float
    
    return prepaid_values


def generate_commit_consumption_data(usage_df: pd.DataFrame, tabs_bt_contract: pd.DataFrame) -> dict:
    """
    Generate commit consumption report data for updating Google Sheet.
    Returns a dictionary mapping (tenant_id, contract_id) to total consumption amount.
    Excludes Prepaid rows.
    
    Calculates: sum(amount_1 * meter_value) per customer/contract
    
    Args:
        usage_df: The usage DataFrame from create_tabs_ready_usage
        tabs_bt_contract: The billing terms contract DataFrame
        
    Returns:
        dict: {(tenant_id, contract_id): total_amount}
    """
    consumption_values = {}
    
    if usage_df is None or usage_df.empty:
        return consumption_values
    
    if tabs_bt_contract is None or tabs_bt_contract.empty:
        return consumption_values
    
    # Exclude Prepaid and Enterprise Support rows
    filtered_usage = usage_df[
        ~usage_df['event_type_name'].str.contains('Prepaid', case=False, na=False)
    ]
    
    if filtered_usage.empty:
        return consumption_values
    
    # Create mapping from customer_id to (tenant_id, contract_name)
    customer_to_info = {}
    if 'customer_id' in tabs_bt_contract.columns:
        for _, row in tabs_bt_contract.iterrows():
            customer_id = str(row.get('customer_id', ''))
            tenant_id = str(row.get('tenant_id', ''))
            contract_name = str(row.get('contract_name', ''))
            if customer_id and customer_id != 'nan':
                customer_to_info[customer_id] = (tenant_id, contract_name)
    
    # Create lookup for amount_1 from tabs_bt_contract
    # Key: (customer_id, name) -> amount_1
    amount_lookup = {}
    for _, row in tabs_bt_contract.iterrows():
        customer_id = str(row.get('customer_id', ''))
        name = str(row.get('name', ''))
        amount_1 = row.get('amount_1', 0)
        
        if customer_id and name and customer_id != 'nan':
            try:
                amount_float = float(str(amount_1).replace(',', '').strip()) if pd.notna(amount_1) else 0
            except (ValueError, TypeError):
                amount_float = 0
            amount_lookup[(customer_id, name)] = amount_float
    
    # Calculate consumption values
    for _, row in filtered_usage.iterrows():
        customer_id = str(row.get('customer_id', ''))
        event_type_name = str(row.get('event_type_name', ''))
        meter_value = row.get('value', 0)
        
        if customer_id not in customer_to_info:
            continue
        
        tenant_id, contract_name = customer_to_info[customer_id]
        
        # Get amount_1 for this (customer_id, event_type_name) combination
        amount_1 = amount_lookup.get((customer_id, event_type_name), 0)
        
        try:
            meter_float = float(str(meter_value).replace(',', '').strip()) if pd.notna(meter_value) else 0
        except (ValueError, TypeError):
            meter_float = 0
        
        # Calculate amount_1 * meter_value
        product = amount_1 * meter_float
        
        key = (tenant_id, contract_name)
        if key in consumption_values:
            consumption_values[key] += product
        else:
            consumption_values[key] = product
    
    return consumption_values