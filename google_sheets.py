"""
Google Sheets API integration for updating Prepaid and Commit Consumption reports.
"""
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import streamlit as st

# Hardcoded Sheet ID for Alkira <> Tabs | Data Templates
SPREADSHEET_ID = "10Znr32hQQRS1qOcVQIqAtg9PU_6ht5z7WjfXyaL47i4"

# Sheet names
PREPAID_SHEET_NAME = "Prepaid Report"
COMMIT_CONSUMPTION_SHEET_NAME = "Commit Consumption Report"

# Scopes required for Google Sheets API
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


def get_gspread_client():
    """
    Authenticate with Google Sheets API using service account credentials.
    
    The service account JSON should be stored in Streamlit secrets or as a file.
    
    Returns:
        gspread.Client: Authenticated gspread client
    """
    try:
        # Try to get credentials from Streamlit secrets
        if 'gcp_service_account' in st.secrets:
            credentials_dict = dict(st.secrets['gcp_service_account'])
            credentials = Credentials.from_service_account_info(
                credentials_dict,
                scopes=SCOPES
            )
        else:
            # Fallback to file-based credentials
            credentials = Credentials.from_service_account_file(
                'service_account.json',
                scopes=SCOPES
            )
        
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        print(f"Error authenticating with Google Sheets: {str(e)}")
        raise


def update_prepaid_sheet(prepaid_values: dict) -> dict:
    """
    Update Column G (Prepaid Amount Consumed) in the Prepaid sheet.
    Adds calculated prepaid values to existing values.
    Updates Column A with timestamp.
    
    Args:
        prepaid_values: Dictionary mapping tenant_id to calculated prepaid value
                       {tenant_id: calculated_prepaid_value}
    
    Returns:
        dict: Result with success status and message
    """
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(PREPAID_SHEET_NAME)
        
        # Get all data from the sheet
        all_data = worksheet.get_all_values()
        
        if len(all_data) < 2:
            return {"success": False, "message": "Sheet has no data rows"}
        
        # Column B (index 1) = Tenant ID
        # Column G (index 6) = Prepaid Amount Consumed
        # Column A (index 0) = Last Updated (timestamp)
        
        tenant_id_col = 1  # Column B (0-indexed)
        prepaid_amount_col = 6  # Column G (0-indexed)
        timestamp_col = 0  # Column A (0-indexed)
        
        # Current timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        updates = []
        rows_updated = 0
        
        # Skip header row (index 0)
        for row_idx, row in enumerate(all_data[1:], start=2):  # start=2 for 1-indexed sheet rows
            if len(row) <= tenant_id_col:
                continue
            
            tenant_id = str(row[tenant_id_col]).strip()
            
            if tenant_id in prepaid_values:
                # Get existing value in Column G
                existing_value = 0
                if len(row) > prepaid_amount_col and row[prepaid_amount_col]:
                    try:
                        existing_value = float(str(row[prepaid_amount_col]).replace(',', '').replace('$', '').strip())
                    except (ValueError, TypeError):
                        existing_value = 0
                
                # Add calculated value to existing
                new_value = existing_value + prepaid_values[tenant_id]
                
                # Queue updates for Column G and Column A
                # Column G (prepaid amount)
                updates.append({
                    'range': f'{_col_letter(prepaid_amount_col + 1)}{row_idx}',
                    'values': [[round(new_value, 2)]]
                })
                # Column A (timestamp)
                updates.append({
                    'range': f'{_col_letter(timestamp_col + 1)}{row_idx}',
                    'values': [[timestamp]]
                })
                rows_updated += 1
        
        # Batch update all cells
        if updates:
            worksheet.batch_update(updates)
        
        return {
            "success": True,
            "message": f"Updated {rows_updated} rows in Prepaid sheet",
            "rows_updated": rows_updated
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error updating Prepaid sheet: {str(e)}"}


def update_commit_consumption_sheet(consumption_values: dict, billing_date: str) -> dict:
    """
    Update the Commit Consumption sheet with monthly totals.
    Finds column matching billing_date + 1 month (MM/DD/YYYY format).
    Updates Column A with timestamp.
    
    Args:
        consumption_values: Dictionary mapping (tenant_id, contract_id) to total amount
                           {(tenant_id, contract_id): total_amount}
        billing_date: Billing run date in YYYY-MM-DD format
    
    Returns:
        dict: Result with success status and message
    """
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(COMMIT_CONSUMPTION_SHEET_NAME)
        
        # Get all data from the sheet
        all_data = worksheet.get_all_values()
        
        if len(all_data) < 2:
            return {"success": False, "message": "Sheet has no data rows"}
        
        # Calculate target date (billing_date + 1 month)
        # Sheet column headers use format like "Oct-2023", "Nov-2023", "Dec-2024" (MMM-YYYY)
        billing_dt = datetime.strptime(billing_date, '%Y-%m-%d')
        target_dt = billing_dt + relativedelta(months=1)
        target_date = target_dt.strftime('%b-%Y')  # e.g., "Dec-2024"
        
        # Find the column with the target date in header row
        header_row = all_data[0]
        target_col = None
        
        for col_idx, header in enumerate(header_row):
            header_clean = str(header).strip()
            # Try exact match first (e.g., "Dec-2024")
            if header_clean == target_date:
                target_col = col_idx
                break
            # Also try case-insensitive match
            if header_clean.lower() == target_date.lower():
                target_col = col_idx
                break
            # Try parsing MMM-YYYY format
            try:
                header_dt = datetime.strptime(header_clean, '%b-%Y')
                if header_dt.strftime('%b-%Y') == target_date:
                    target_col = col_idx
                    break
            except ValueError:
                continue
        
        if target_col is None:
            return {
                "success": False, 
                "message": f"Could not find column with date {target_date} in header row. Available columns: {header_row[:25]}"
            }
        
        # Column B (index 1) = SFDC# Contract ID
        # Column C (index 2) = Tenant ID
        # Column A (index 0) = Last Updated (timestamp)
        
        contract_id_col = 1  # Column B (0-indexed)
        tenant_id_col = 2    # Column C (0-indexed)
        timestamp_col = 0    # Column A (0-indexed)
        
        # Current timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        updates = []
        rows_updated = 0
        
        # Skip header row (index 0)
        for row_idx, row in enumerate(all_data[1:], start=2):  # start=2 for 1-indexed sheet rows
            if len(row) <= max(contract_id_col, tenant_id_col):
                continue
            
            contract_id = str(row[contract_id_col]).strip()
            tenant_id = str(row[tenant_id_col]).strip()
            
            lookup_key = (tenant_id, contract_id)
            
            if lookup_key in consumption_values:
                value = consumption_values[lookup_key]
                
                # Queue updates for target column and Column A
                # Target column (consumption value)
                updates.append({
                    'range': f'{_col_letter(target_col + 1)}{row_idx}',
                    'values': [[round(value, 2)]]
                })
                # Column A (timestamp)
                updates.append({
                    'range': f'{_col_letter(timestamp_col + 1)}{row_idx}',
                    'values': [[timestamp]]
                })
                rows_updated += 1
        
        # Batch update all cells
        if updates:
            worksheet.batch_update(updates)
        
        return {
            "success": True,
            "message": f"Updated {rows_updated} rows in Commit Consumption sheet (column: {target_date})",
            "rows_updated": rows_updated,
            "target_column": target_date
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error updating Commit Consumption sheet: {str(e)}"}


def _col_letter(col_num: int) -> str:
    """
    Convert column number to letter (1 = A, 2 = B, ... 27 = AA, etc.)
    
    Args:
        col_num: 1-indexed column number
    
    Returns:
        str: Column letter(s)
    """
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result

