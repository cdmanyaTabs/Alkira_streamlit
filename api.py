import pandas as pd
import streamlit as st
import requests
import io
import json

def get_customer_custom_field():
    url = f"https://integrators.prod.api.tabsplatform.com/v3/customers/custom-fields"
    headers = {
        "Authorization": f"{st.session_state['tabs_api_key']}"
    }
    response = requests.get(url, headers=headers)
    return response.json()
def get_all_customers():
    url = f"https://integrators.prod.api.tabsplatform.com/v3/customers?limit=10000"
    headers = {
        "Authorization": f"{st.session_state['tabs_api_key']}"
    }
    response = requests.get(url+"1", headers=headers)
    success = response.json().get("success")
    if success:
        payload = response.json().get("payload",{})
        totalItems = payload.get("totalItems",0)
        if totalItems > 0:
            response = requests.get(url+str(totalItems), headers=headers)
            if response.json().get("success"):
                payload = response.json().get("payload",{})
                data = payload.get("data",[])
                return data
    return []

    # Example response:
    #  "payload": {
    # "data": [
    #   {
    #     "id": "a8e03b2b-8b20-4eb7-bdac-da1c1447dab2",
    #     "name": "Commit Consume Customer No. 3",
    #     "parentCustomerId": null,
    #     "secondaryBillingContacts": [],
    #     "externalIds": [],
    #     "defaultCurrency": "USD",
    #     "lastUpdatedAt": "2024-08-15T19:27:52.828Z",
       # "customFields": [
        #   {
        #     "id": "3ecee77e-eba1-4142-acc2-42290b1958b5",
        #     "manufacturerCustomFieldId": "6ddd8eff-818d-4462-a369-3912576b3b84",
        #     "customFieldName": "Tenant ID",
        #     "customFieldValue": "449"
        #   }
        # ]    #   },
    #   {
    #     "id": "a22deb49-03d1-4490-9907-da2fe883d8cd",
    #     "name": "Commit Consume Customer No. 4",
    #     "parentCustomerId": null,
    #     "secondaryBillingContacts": [],
    #     "externalIds": [],
    #     "defaultCurrency": "USD",
    #     "lastUpdatedAt": "2024-08-15T19:27:34.659Z",
        # "customFields": [
        #   {
        #     "id": "3ecee77e-eba1-4142-acc2-42290b1958b5",
        #     "manufacturerCustomFieldId": "6ddd8eff-818d-4462-a369-3912576b3b84",
        #     "customFieldName": "Tenant ID",
        #     "customFieldValue": "449"
        #   }
        # ]
    #   },

def get_event_ids():
    url = "https://integrators.prod.api.tabsplatform.com/v3/events/types?limit=1000"
    headers = {
        "Authorization": f"{st.session_state['tabs_api_key']}"
    }
    response = requests.get(url, headers=headers)
    return pd.DataFrame(response.json().get("payload",{}).get("data",[]))
 
#    # Example response:
#    {
#   "payload": {
#     "data": [
#       {
#         "id": "0000802f-5b90-4610-8434-99ac8dae5497",
#         "name": "GCP Interconnect - L"
#       },
#       {
#         "id": "00d41157-2098-4831-9043-1ca39a97f719",
#         "name": "PAN - BYOL - M"
#       },
#       {
#         "id": "012b7a24-3828-4c48-ab30-2a0ea3cd3602",
#         "name": "FortiGate - BYOL - M"
#       }]}}    

def get_integration_items():
    url = "https://integrators.prod.api.tabsplatform.com/v3/items?limit=1000"
    headers = {
        "Authorization": f"{st.session_state['tabs_api_key']}"
    }
    response = requests.get(url, headers=headers)
    return pd.DataFrame(response.json().get("payload",{}).get("data",[]))
    # Example response:
#     {
#   "payload": {
#     "data": [
#       {
#         "id": "3b96a3da-480e-4d22-a524-52c3a14b3037",
#         "name": "Additional Alkira Datastore - 100G",
#         "externalIds": [
#           {
#             "type": "NETSUITE",
#             "id": "110"
#           }
#         ]
#       },
#       {
#         "id": "eabe1261-76aa-492c-bbf9-ee0cf3153b04",
#         "name": "Akamai Prolexic - 2L",
#         "externalIds": [
#           {
#             "type": "NETSUITE",
#             "id": "1920"
#           }
#         ]
#       },
#       ]}}

def find_contracts(customer_id, contract_name):
    contracts = st.session_state["all_contracts"]
    matching_contracts = [
        contract for contract in contracts
        if contract["customerId"] == customer_id and contract["name"] == contract_name
    ]
    need_to_create = not matching_contracts  # True if no matches found

    return matching_contracts, need_to_create


def create_contract(customer_id, contract_name):
    url = f"/v3/contracts"
    create_contract_url = f"https://integrators.prod.api.tabsplatform.com{url}"
    payload = {
            "name": contract_name,
            "customerId": customer_id,
            "shouldProcess": False
    }
    headers = {
        "Authorization": f"{st.session_state['tabs_api_key']}"
    }
    response = requests.post(create_contract_url, headers=headers, json=payload)
    if response.status_code == 201 or response.status_code == 200:
        print(f"✓ create_contract API call successful (HTTP {response.status_code}) for customer {customer_id}")
        if hasattr(response, 'json'):
            response_data = response.json()
            full_payload = response_data.get("payload", {})
            contract_id = full_payload.get("id")
            if not contract_id:
                print(f"✗ create_contract: No contract_id in response for customer {customer_id}")
                ret = None
            else:
                actionpayload = {
                    "action": "MARK_AS_PROCESSED"
                }
                action_response = requests.post(f"https://integrators.prod.api.tabsplatform.com/v3/contracts/{contract_id}/actions", json = actionpayload, headers=headers)
                if action_response.status_code == 200 or action_response.status_code == 201:
                    print(f"✓ create_contract: Contract {contract_id} marked as processed for customer {customer_id}")
                else:
                    print(f"⚠ create_contract: Failed to mark contract {contract_id} as processed (HTTP {action_response.status_code})")
                ret = contract_id, full_payload
        else:
            print(f"✗ create_contract: Invalid response format for customer {customer_id}")
            ret = None
    else:
        print(f"✗ create_contract API call failed (HTTP {response.status_code}) for customer {customer_id}")
        try:
            error_data = response.json()
            error_msg = error_data.get('message', error_data.get('error', 'Unknown error'))
            print(f"  Error details: {error_msg}")
        except:
            print(f"  Error details: {response.text if hasattr(response, 'text') else 'No error details available'}")
        ret = None
    return ret

    
# ============================================================================
# OLD IMPLEMENTATION - COMMENTED OUT
# ============================================================================
# def push_bt(csv_file_data, merchant_name='alkira'):
#     """
#     Push CSV file data to bulk-create-billing-schedules endpoint as multipart/form-data.
#     
#     Args:
#         csv_file_data: Tuple (filename, file_data, content_type) or file-like object for CSV upload
#         merchant_name: Merchant name for the endpoint (default: 'alkira')
#         
#     Returns:
#         Response object from the API
#     """
#     #prep the url
#     url = f"https://integrators.prod.api.tabsplatform.com/v16/secrets/merchant/{merchant_name}/bulk-create-billing-schedules"
# 
#     #prep the header
#     headers = {
#         "Authorization": f"{st.session_state['tabs_api_key']}"
#     }
# 
#     # Prepare files for multipart/form-data upload
#     # csv_file_data should be a tuple of (filename, file_data, content_type) or file-like object
#     if isinstance(csv_file_data, tuple):
#         files = {'file': csv_file_data}
#     else:
#         # If it's a file-like object, wrap it with a filename
#         files = {'file': ('billing_schedules.csv', csv_file_data, 'text/csv')}
#     
#     # Send CSV file as multipart/form-data
#     response = requests.post(url, headers=headers, files=files)
#     
#     # Print status of push_bt API call
#     if response.status_code == 201:
#         print(f"✓ push_bt API call successful (HTTP {response.status_code})")
#         try:
#             response_data = response.json()
#             # Debug: Print the full response structure
#             print(f"  Full API response: {response_data}")
#             
#             # Check if billingTermIds is nested under payload or data
#             billing_term_ids = response_data.get('billingTermIds', [])
#             if not billing_term_ids and 'payload' in response_data:
#                 payload = response_data.get('payload', {})
#                 billing_term_ids = payload.get('billingTermIds', [])
#             if not billing_term_ids and 'data' in response_data:
#                 data = response_data.get('data', {})
#                 billing_term_ids = data.get('billingTermIds', [])
#             
#             if billing_term_ids:
#                 print(f"  Created {len(billing_term_ids)} billing term(s)")
#             else:
#                 print(f"  Warning: No billingTermIds in response")
#                 print(f"  Response keys: {list(response_data.keys())}")
#         except Exception as e:
#             print(f"  Warning: Could not parse response JSON: {str(e)}")
#             print(f"  Response text: {response.text[:500] if hasattr(response, 'text') else 'N/A'}")
#     else:
#         print(f"✗ push_bt API call failed (HTTP {response.status_code})")
#         try:
#             error_data = response.json()
#             error_msg = error_data.get('message', error_data.get('error', 'Unknown error'))
#             print(f"  Error details: {error_msg}")
#         except:
#             print(f"  Error details: {response.text if hasattr(response, 'text') else 'No error details available'}")
#     
#     return response

def push_bt(csv_file_data, merchant_name='alkira'):
    """
    Push billing terms to Tabs API using the new v3/contracts/{id}/obligations endpoint.
    Parses CSV data and creates individual obligations for each row.
    
    Args:
        csv_file_data: Tuple (filename, file_data, content_type) or file-like object for CSV upload
        merchant_name: Merchant name (kept for backward compatibility, not used in new API)
        
    Returns:
        Response-like object compatible with old implementation
    """
    # Parse CSV data to DataFrame
    try:
        if isinstance(csv_file_data, tuple):
            # Extract file data from tuple (filename, file_data, content_type)
            csv_string = csv_file_data[1]
            if isinstance(csv_string, bytes):
                csv_string = csv_string.decode('utf-8')
        else:
            # If it's a file-like object, read it
            csv_string = csv_file_data.read()
            if isinstance(csv_string, bytes):
                csv_string = csv_string.decode('utf-8')
        
        # Convert CSV string to DataFrame
        csv_buffer = io.StringIO(csv_string)
        df = pd.read_csv(csv_buffer)
        
    except Exception as e:
        print(f"✗ push_bt: Failed to parse CSV data: {str(e)}")
        # Return a mock response object with error status
        class MockResponse:
            def __init__(self, status_code, error_msg):
                self.status_code = status_code
                self._error_msg = error_msg
            def json(self):
                return {"error": self._error_msg}
        return MockResponse(400, f"Failed to parse CSV: {str(e)}")
    
    # Prepare headers
    headers = {
        "Authorization": f"{st.session_state['tabs_api_key']}",
        "Content-Type": "application/json"
    }
    
    # Collect obligation IDs and track results
    obligation_ids = []
    errors = []
    total_rows = len(df)
    successful_count = 0
    failed_count = 0
    
    # Process each row
    for idx, row in df.iterrows():
        try:
            # Extract contract_id
            contract_id = row.get('contract_id', '')
            if not contract_id or pd.isna(contract_id):
                error_msg = f"Row {idx + 1}: Missing contract_id"
                print(f"✗ push_bt: {error_msg}")
                errors.append(error_msg)
                failed_count += 1
                continue
            
            # Build URL
            url = f"https://integrators.prod.api.tabsplatform.com/v3/contracts/{contract_id}/obligations"
            
            # Extract and map DataFrame columns to API payload
            service_start_date = row.get('revenue_start_date', '')
            service_end_date = row.get('revenue_end_date', '')
            name = row.get('name', '')
            note = row.get('note', '') if pd.notna(row.get('note', '')) else ''
            invoice_date = row.get('invoice_date', '')
            duration = row.get('duration', 1)
            net_payment_terms = row.get('net_payment_terms', '')
            amount = row.get('amount_1', 0)
            event_type_id = row.get('event_to_track', '')
            item_id = row.get('integration_item_id', '')
            
            # Handle NaN values - convert to appropriate defaults to avoid JSON serialization errors
            if pd.isna(name):
                name = ''
            if pd.isna(net_payment_terms):
                net_payment_terms = ''
            if pd.isna(event_type_id):
                event_type_id = ''
            
            # Convert duration to int if it's not already
            try:
                duration = int(duration) if pd.notna(duration) else 1
            except (ValueError, TypeError):
                duration = 1
            
            # Convert amount to float
            try:
                amount = float(amount) if pd.notna(amount) else 0.0
            except (ValueError, TypeError):
                amount = 0.0
            
            # Handle date formatting - ensure YYYY-MM-DD format
            def format_date(date_value):
                if pd.isna(date_value) or date_value == '':
                    return None
                date_str = str(date_value).strip()
                # If it's already in YYYY-MM-DD format, return as is
                if len(date_str) == 10 and date_str.count('-') == 2:
                    return date_str
                # Try to parse and reformat
                try:
                    from datetime import datetime
                    # Try common date formats
                    for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']:
                        try:
                            dt = datetime.strptime(date_str, fmt)
                            return dt.strftime('%Y-%m-%d')
                        except ValueError:
                            continue
                    return date_str  # Return as-is if parsing fails
                except:
                    return date_str
            
            service_start_date = format_date(service_start_date)
            service_end_date = format_date(service_end_date)
            invoice_date = format_date(invoice_date)
            
            # Build payload
            payload = {
                "serviceStartDate": service_start_date,
                "serviceEndDate": service_end_date,
                "billingSchedule": {
                    "name": name,
                    "description": note,
                    "startDate": invoice_date,
                    "duration": duration,
                    "invoiceDateStrategy": "ARREARS",
                    "isRecurring": True,
                    "interval": "MONTH",
                    "intervalFrequency": 1,
                    "netPaymentTerms": net_payment_terms,
                    "quantity": 1,
                    "billingType": "UNIT",
                    "pricingType": "SIMPLE",
                    "eventTypeId": event_type_id,
                    "invoiceType": "INVOICE",
                    "pricing": [
                        {
                            "tier": 1,
                            "amount": amount,
                            "amountType": "PER_ITEM",
                            "tierMinimum": 0
                        }
                    ]
                }
            }
            
            # Add itemId if available
            if item_id and pd.notna(item_id) and str(item_id).strip():
                payload["billingSchedule"]["itemId"] = str(item_id).strip()
            
            # Make POST request
            response = requests.post(url, headers=headers, json=payload)
            
            # Handle response
            if response.status_code == 201 or response.status_code == 200:
                try:
                    response_data = response.json()
                    # Extract obligation ID from response
                    # The ID might be in payload.id or directly in the response
                    obligation_id = None
                    if 'payload' in response_data:
                        payload_data = response_data.get('payload', {})
                        obligation_id = payload_data.get('id')
                    if not obligation_id:
                        obligation_id = response_data.get('id')
                    
                    if obligation_id:
                        obligation_ids.append(obligation_id)
                        successful_count += 1
                        print(f"✓ push_bt: Created obligation {obligation_id} for contract {contract_id} (row {idx + 1})")
                    else:
                        print(f"⚠ push_bt: API call succeeded for row {idx + 1} but no obligation ID in response")
                        successful_count += 1
                except Exception as e:
                    print(f"⚠ push_bt: API call succeeded for row {idx + 1} but failed to parse response: {str(e)}")
                    successful_count += 1
            else:
                error_msg = f"Row {idx + 1}: HTTP {response.status_code}"
                try:
                    error_data = response.json()
                    # Try multiple possible error fields
                    error_msg = (
                        error_data.get('message') or 
                        error_data.get('error') or 
                        str(error_data.get('errors', '')) or
                        str(error_data.get('payload', {}).get('message', '')) or
                        str(error_data)
                    )
                    # Print full error response for debugging
                    print(f"  Debug - Full error response: {error_data}")
                except:
                    error_msg = response.text[:500] if hasattr(response, 'text') else error_msg
                    print(f"  Debug - Error response text: {error_msg}")
                
                # Also print the payload that was sent for debugging (without sensitive data)
                print(f"  Debug - Payload sent: contract_id={contract_id}, name={name}, amount={amount}, event_type_id={event_type_id}, service_start_date={service_start_date}, service_end_date={service_end_date}")
                
                print(f"✗ push_bt: Failed to create obligation for contract {contract_id} (row {idx + 1}): {error_msg}")
                errors.append(f"Row {idx + 1}: {error_msg}")
                failed_count += 1
                
        except Exception as e:
            error_msg = f"Row {idx + 1}: {str(e)}"
            print(f"✗ push_bt: Error processing row {idx + 1}: {str(e)}")
            errors.append(error_msg)
            failed_count += 1
    
    # Print summary
    print(f"✓ push_bt: Processed {total_rows} row(s) - {successful_count} successful, {failed_count} failed")
    if obligation_ids:
        print(f"✓ push_bt: Created {len(obligation_ids)} obligation(s)")
    
    # Create a mock response object compatible with old implementation
    class MockResponse:
        def __init__(self, status_code, obligation_ids, errors):
            self.status_code = status_code
            self._obligation_ids = obligation_ids
            self._errors = errors
        
        def json(self):
            # Return structure compatible with old implementation
            return {
                "billingTermIds": self._obligation_ids,
                "errors": self._errors if self._errors else None
            }
    
    # Return success if at least some obligations were created
    status_code = 201 if successful_count > 0 else 400
    return MockResponse(status_code, obligation_ids, errors)
