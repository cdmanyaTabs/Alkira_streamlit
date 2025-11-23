from typing import Any
import zipfile
import io
import re
import json
import pandas as pd
from datetime import datetime, timedelta
from api import get_all_customers, get_event_ids, get_integration_items, create_contract, push_bt

def price_book_transformation(zip_file, billing_run_date=None):
    """
    Extract ZIP file and parse tenant IDs from filenames.
    Supports ZIP files containing either CSV files or XLSX/XLS files (or both).
    
    Filename format: price_by_sku_40_Koch_SFDC#00000190.xlsx (or .csv)
    where 40 is the tenant_id (number after 'price_by_sku_')
    
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
        
        # Filter for both CSV and Excel files
        data_files = [f for f in file_list if f.endswith(('.csv', '.xlsx', '.xls'))]
        
        # Parse each filename to extract tenant_id
        for filename in data_files:
            # Pattern: price_by_sku_40_Koch_SFDC#00000190.xlsx or .csv
            # Extract the number after 'price_by_sku_'
            match = re.search(r'price_by_sku_(\d+)_', filename)
            
            if match:
                tenant_id = match.group(1)
                
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
                    
                    # Create case-insensitive mapping
                    df_columns_lower = {col.lower(): col for col in df_full.columns}
                    
                    # Find matching columns and check for missing ones
                    matched_columns = []
                    missing_columns = []
                    
                    for req_col in required_columns:
                        req_col_lower = req_col.lower()
                        if req_col_lower in df_columns_lower:
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
                        'tenant_id': tenant_id
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
                    
                    # Find the Tenant ID custom field
                    for field in custom_fields:
                        if field.get('customFieldName') == 'Tenant ID':
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
                    
                    # Create mapping dictionary: event_name -> event_id
                    event_name_to_id = {}
                    if not events_df.empty and 'name' in events_df.columns and 'id' in events_df.columns:
                        for _, row in events_df.iterrows():
                            event_name = row.get('name')
                            event_id = row.get('id')
                            if event_name and event_id:
                                event_name_to_id[event_name] = event_id
                    
                    # Update event_to_track in all DataFrames
                    for df in all_dataframes:
                        if 'SKU Name' in df.columns:
                            df['event_to_track'] = df['SKU Name'].map(event_name_to_id)
                            
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
    filtered_columns = ['Tabs Customer ID', 'event_to_track', 'integration_item_id', 'SKU Name', 'NET RATE', 'tenant_id', 'Net Terms']
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
    Filter filtered_df to only include rows where tenant_id and name match
    the Tenant ID and SKU Name from the raw monthly usage file.
    
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
        
        # Check if required columns exist in raw usage file
        required_columns = ['Tenant ID', 'SKU Name']
        missing_columns = [col for col in required_columns if col not in raw_usage_df.columns]
        
        if missing_columns:
            print(f"Error: Missing required columns in raw monthly usage file: {', '.join(missing_columns)}")
            return filtered_df
        
        # Get unique combinations of Tenant ID and SKU Name from raw usage file
        # Convert to string and handle any NaN values
        raw_usage_df['Tenant ID'] = raw_usage_df['Tenant ID'].astype(str)
        raw_usage_df['SKU Name'] = raw_usage_df['SKU Name'].astype(str)
        
        # Create a set of tuples for matching (Tenant ID, SKU Name)
        matching_pairs = set(zip(raw_usage_df['Tenant ID'], raw_usage_df['SKU Name']))
        
        # Make a copy of filtered_df to avoid modifying the original
        filtered_df_copy = filtered_df.copy()
        
        # Convert tenant_id and name to string for comparison
        filtered_df_copy['tenant_id'] = filtered_df_copy['tenant_id'].astype(str)
        filtered_df_copy['name'] = filtered_df_copy['name'].astype(str)
        
        # Filter filtered_df to only include rows where (tenant_id, name) matches
        # any combination in the raw usage file
        mask = filtered_df_copy.apply(
            lambda row: (str(row['tenant_id']), str(row['name'])) in matching_pairs,
            axis=1
        )
        
        # Create Tabs_bt_final_df with only matching rows
        Tabs_bt_clean_df = filtered_df_copy[mask].copy()
        
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
                'event_to_track': 'f62255fd-9a75-4e23-b8bd-d39500334d22',
                'name': 'Enterprise Support',
                'note': '',
                'integration_item_id': 'c9893624-ea6d-495f-8a8e-38fa4ef75050',
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
        
        # Check if prepaid file has at least 1 column (Column A for Tenant ID)
        if len(prepaid_df.columns) < 1:
            print(f"Error: Prepaid file must have at least 1 column (Column A for Tenant ID)")
            print(f"File has {len(prepaid_df.columns)} column(s)")
            return tabs_bt_enterprise
        
        # Use column A (index 0) for Tenant ID
        tenant_id_col = prepaid_df.columns[0]
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
                'event_to_track': '16cb100d-4e22-41c8-bc06-603729e819ea',
                'name': 'Prepaid',  
                'note': '',
                'integration_item_id': '24ab1afb-a18f-4fe5-8ba4-a6c274d89139',
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
    Generate CSV-ready DataFrame from raw monthly usage file.
    Maps Tenant ID to Tabs customer_id and creates output with customer_id, SKU Name, datetime, and Meter.
    Filters output to only include customer_ids that exist in tabs_bt_contract.
    Adds Enterprise Support rows and calculates their values using Enterprise Support %.
    
    Args:
        raw_monthly_usage_file: UploadedFile object from Streamlit file_uploader (CSV, XLSX, or XLS)
        tabs_bt_contract: DataFrame with contract information containing customer_id column
        enterprise_support_file: Optional UploadedFile object from Streamlit file_uploader (CSV, XLSX, or XLS) with Enterprise Support % data
        billing_run_date: Optional billing run date in YYYY-MM-DD format (defaults to date two weeks ago if not provided)
        
    Returns:
        pd.DataFrame: DataFrame with columns: customer_id, event_type_name, datetime, value, differentiator
    """
    if raw_monthly_usage_file is None:
        return pd.DataFrame()
    
    if tabs_bt_contract is None or tabs_bt_contract.empty:
        print("Warning: tabs_bt_contract is empty or None, cannot filter by customer_id")
        return pd.DataFrame()
    
    try:
        # Read the raw monthly usage file based on file extension
        file_extension = raw_monthly_usage_file.name.split('.')[-1].lower()
        
        if file_extension == 'csv':
            raw_usage_df = pd.read_csv(raw_monthly_usage_file)
        elif file_extension in ['xlsx', 'xls']:
            raw_usage_df = pd.read_excel(raw_monthly_usage_file)
        else:
            print(f"Unsupported file type: {file_extension}")
            return pd.DataFrame()
        
        # Check if required columns exist in raw usage file
        required_columns = ['Tenant ID', 'SKU Name', 'Meter']
        missing_columns = [col for col in required_columns if col not in raw_usage_df.columns]
        
        if missing_columns:
            print(f"Error: Missing required columns in raw monthly usage file: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # Convert Tenant ID to string for matching
        raw_usage_df['Tenant ID'] = raw_usage_df['Tenant ID'].astype(str)
        
        # Get all customers from API to map tenant_id to Tabs Customer ID
        try:
            customers_data = get_all_customers()
            
            # Create mapping dictionary: tenant_id -> tabs_customer_id
            tenant_to_customer_id = {}
            for customer in customers_data:
                tabs_customer_id = customer.get('id')
                custom_fields = customer.get('customFields', [])
                
                # Find the Tenant ID custom field
                for field in custom_fields:
                    if field.get('customFieldName') == 'Tenant ID':
                        tenant_id_value = field.get('customFieldValue')
                        if tenant_id_value:
                            tenant_to_customer_id[str(tenant_id_value)] = tabs_customer_id
                            break
            
            # Map Tenant ID to customer_id
            raw_usage_df['customer_id'] = raw_usage_df['Tenant ID'].map(tenant_to_customer_id)
            
            # Check for unmatched tenant IDs
            unmatched_rows = raw_usage_df[raw_usage_df['customer_id'].isna()]
            if not unmatched_rows.empty:
                unmatched_tenant_ids = unmatched_rows['Tenant ID'].unique()
                print(f"Warning: No matching Tabs Customer ID found for tenant ID(s): {', '.join(sorted(unmatched_tenant_ids, key=str))}")
            
            # Filter out rows where customer_id mapping failed
            output_df = raw_usage_df[raw_usage_df['customer_id'].notna()].copy()
            
            if output_df.empty:
                print("Warning: No rows with valid customer_id mappings found")
                return pd.DataFrame()
            
        except Exception as e:
            print(f"Error fetching customers from API: {str(e)}")
            return pd.DataFrame()
        
        # Use billing_run_date if provided, otherwise use date two weeks ago
        if billing_run_date:
            # Validate date format
            try:
                datetime.strptime(billing_run_date, '%Y-%m-%d')
                output_df['datetime'] = billing_run_date
            except ValueError:
                print(f"Warning: Invalid billing_run_date format. Expected YYYY-MM-DD, got: {billing_run_date}")
                output_df['datetime'] = (datetime.now() - timedelta(weeks=2)).strftime('%Y-%m-%d')
        else:
            output_df['datetime'] = (datetime.now() - timedelta(weeks=2)).strftime('%Y-%m-%d')
        
        # Select only the required columns: customer_id, SKU Name, datetime, Meter
        # (will be renamed to event_type_name and value)
        output_df = output_df[['customer_id', 'SKU Name', 'datetime', 'Meter']].copy()
        
        # Rename columns: SKU Name -> event_type_name, Meter -> value
        output_df = output_df.rename(columns={
            'SKU Name': 'event_type_name',
            'Meter': 'value'
        })
        
        # Add differentiator column with blank values
        output_df['differentiator'] = ''
        
        # Reset index
        output_df = output_df.reset_index(drop=True)
        
        # Filter output_df to only include customer_ids that exist in tabs_bt_contract
        if 'customer_id' in tabs_bt_contract.columns:
            # Get unique customer_ids from tabs_bt_contract
            valid_customer_ids = tabs_bt_contract['customer_id'].unique()
            
            # Convert both to string for reliable matching
            valid_customer_ids = set(str(cid) for cid in valid_customer_ids if pd.notna(cid))
            output_df['customer_id'] = output_df['customer_id'].astype(str)
            
            # Store original count before filtering
            original_count = len(output_df)
            
            # Filter output_df to only include rows where customer_id is in tabs_bt_contract
            output_df = output_df[output_df['customer_id'].isin(valid_customer_ids)].copy()
            
            # Reset index after filtering
            output_df = output_df.reset_index(drop=True)
            
            # Warn if any rows were filtered out
            filtered_count = original_count - len(output_df)
            if filtered_count > 0:
                print(f"Warning: Filtered out {filtered_count} row(s) with customer_ids not found in tabs_bt_contract")
        else:
            print("Warning: 'customer_id' column not found in tabs_bt_contract, skipping filter")
        
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
                                
                                # Find the Tenant ID custom field
                                for field in custom_fields:
                                    if field.get('customFieldName') == 'Tenant ID':
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
        
        # Add rows for Enterprise Support customer_ids
        if 'name' in tabs_bt_contract.columns:
            # Find rows where name contains "Enterprise Support"
            enterprise_rows = tabs_bt_contract[tabs_bt_contract['name'].str.contains('Enterprise Support', case=False, na=False)]
            
            if not enterprise_rows.empty:
                # Get unique customer_ids from Enterprise Support rows
                enterprise_customer_ids = enterprise_rows['customer_id'].unique()
                enterprise_customer_ids = [str(cid) for cid in enterprise_customer_ids if pd.notna(cid)]
                
                if enterprise_customer_ids:
                    # Get datetime from output_df (use first row's value if available)
                    if not output_df.empty and 'datetime' in output_df.columns:
                        billing_date = output_df['datetime'].iloc[0]
                    elif billing_run_date:
                        billing_date = billing_run_date
                    else:
                        billing_date = (datetime.now() - timedelta(weeks=2)).strftime('%Y-%m-%d')
                    
                    # Create new rows for each Enterprise Support customer_id
                    enterprise_new_rows = []
                    for customer_id in enterprise_customer_ids:
                        # Check if this customer_id + event_type_name combination already exists
                        existing = output_df[(output_df['customer_id'] == customer_id) & 
                                           (output_df['event_type_name'] == 'Enterprise Support')]
                        
                        # Only add if it doesn't already exist
                        if existing.empty:
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
                                    sum_product = 0
                                    for _, row in customer_rows.iterrows():
                                        event_type_name = row['event_type_name']
                                        value = row['value']
                                        
                                        # Try to convert value to float
                                        try:
                                            value_float = float(str(value).replace(',', '').strip()) if pd.notna(value) and str(value).strip() else 0
                                        except (ValueError, TypeError):
                                            value_float = 0
                                        
                                        # Find matching row in tabs_bt_contract by customer_id and name/event_type_name
                                        matching_contract_rows = tabs_bt_contract[
                                            (tabs_bt_contract['customer_id'].astype(str) == customer_id) &
                                            (tabs_bt_contract['name'].astype(str) == str(event_type_name))
                                        ]
                                        
                                        if not matching_contract_rows.empty:
                                            # Get amount_1 from first matching row
                                            amount_1 = matching_contract_rows.iloc[0].get('amount_1', 0)
                                            try:
                                                amount_1_float = float(str(amount_1).replace(',', '').strip()) if pd.notna(amount_1) and str(amount_1).strip() else 0
                                            except (ValueError, TypeError):
                                                amount_1_float = 0
                                            
                                            # Calculate value * amount_1
                                            sum_product += value_float * amount_1_float
                                    
                                    # Multiply sum by Enterprise Support % (already in decimal form)
                                    calculated_value = sum_product * enterprise_pct
                            
                            new_row = {
                                'customer_id': customer_id,
                                'event_type_name': 'Enterprise Support',
                                'datetime': billing_date,
                                'value': calculated_value,
                                'differentiator': ''
                            }
                            enterprise_new_rows.append(new_row)
                    
                    # Append new Enterprise Support rows to output_df
                    if enterprise_new_rows:
                        enterprise_df = pd.DataFrame(enterprise_new_rows)
                        output_df = pd.concat([output_df, enterprise_df], ignore_index=True)
                        print(f"Added {len(enterprise_new_rows)} Enterprise Support row(s) to output_df")
        
        # Reset index after adding Enterprise Support rows
        output_df = output_df.reset_index(drop=True)
        
        # Add rows for Prepaid customer_ids
        if 'name' in tabs_bt_contract.columns:
            # Find rows where name contains "Prepaid"
            prepaid_rows = tabs_bt_contract[tabs_bt_contract['name'].str.contains('Prepaid', case=False, na=False)]
            
            if not prepaid_rows.empty:
                # Get unique customer_ids from Prepaid rows
                prepaid_customer_ids = prepaid_rows['customer_id'].unique()
                prepaid_customer_ids = [str(cid) for cid in prepaid_customer_ids if pd.notna(cid)]
                
                if prepaid_customer_ids:
                    # Get datetime from output_df (use first row's value if available)
                    if not output_df.empty and 'datetime' in output_df.columns:
                        billing_date = output_df['datetime'].iloc[0]
                    elif billing_run_date:
                        billing_date = billing_run_date
                    else:
                        billing_date = (datetime.now() - timedelta(weeks=2)).strftime('%Y-%m-%d')
                    
                    # Create new rows for each Prepaid customer_id
                    prepaid_new_rows = []
                    for customer_id in prepaid_customer_ids:
                        # Check if this customer_id + event_type_name combination already exists
                        existing = output_df[(output_df['customer_id'] == customer_id) & 
                                           (output_df['event_type_name'] == 'Prepaid')]
                        
                        # Only add if it doesn't already exist
                        if existing.empty:
                            # Calculate value using formula: sum(value * amount_1) for all customer rows (including Enterprise Support)
                            calculated_value = 0
                            
                            # Find all rows in output_df with this customer_id (INCLUDING Enterprise Support rows)
                            customer_rows = output_df[output_df['customer_id'] == customer_id]
                            
                            if not customer_rows.empty:
                                # Match each row to tabs_bt_contract to get amount_1
                                sum_product = 0
                                for _, row in customer_rows.iterrows():
                                    event_type_name = row['event_type_name']
                                    value = row['value']
                                    
                                    # Try to convert value to float
                                    try:
                                        value_float = float(str(value).replace(',', '').strip()) if pd.notna(value) and str(value).strip() else 0
                                    except (ValueError, TypeError):
                                        value_float = 0
                                    
                                    # Find matching row in tabs_bt_contract by customer_id and name/event_type_name
                                    matching_contract_rows = tabs_bt_contract[
                                        (tabs_bt_contract['customer_id'].astype(str) == customer_id) &
                                        (tabs_bt_contract['name'].astype(str) == str(event_type_name))
                                    ]
                                    
                                    if not matching_contract_rows.empty:
                                        # Get amount_1 from first matching row
                                        amount_1 = matching_contract_rows.iloc[0].get('amount_1', 0)
                                        try:
                                            amount_1_float = float(str(amount_1).replace(',', '').strip()) if pd.notna(amount_1) and str(amount_1).strip() else 0
                                        except (ValueError, TypeError):
                                            amount_1_float = 0
                                        
                                        # Calculate value * amount_1
                                        sum_product += value_float * amount_1_float
                                
                                # Set calculated value as sum of all products
                                calculated_value = sum_product
                            
                            new_row = {
                                'customer_id': customer_id,
                                'event_type_name': 'Prepaid',
                                'datetime': billing_date,
                                'value': calculated_value,
                                'differentiator': ''
                            }
                            prepaid_new_rows.append(new_row)
                    
                    # Append new Prepaid rows to output_df
                    if prepaid_new_rows:
                        prepaid_df = pd.DataFrame(prepaid_new_rows)
                        output_df = pd.concat([output_df, prepaid_df], ignore_index=True)
                        print(f"Added {len(prepaid_new_rows)} Prepaid row(s) to output_df")
        
        # Reset index after adding Prepaid rows
        output_df = output_df.reset_index(drop=True)
        
        print(f"Successfully created usage DataFrame with {len(output_df)} rows")
        return output_df
        
    except Exception as e:
        print(f"Error processing raw monthly usage file: {str(e)}")
        return pd.DataFrame()