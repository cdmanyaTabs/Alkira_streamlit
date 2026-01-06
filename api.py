import pandas as pd
import streamlit as st
import requests
import io
import json
import uuid

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

def get_event_ids():
    url = "https://integrators.prod.api.tabsplatform.com/v3/events/types?limit=1000"
    headers = {
        "Authorization": f"{st.session_state['tabs_api_key']}"
    }
    response = requests.get(url, headers=headers)
    return pd.DataFrame(response.json().get("payload",{}).get("data",[]))
 

def get_integration_items():
    url = "https://integrators.prod.api.tabsplatform.com/v3/items?limit=1000"
    headers = {
        "Authorization": f"{st.session_state['tabs_api_key']}"
    }
    response = requests.get(url, headers=headers)
    return pd.DataFrame(response.json().get("payload",{}).get("data",[]))

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


def mark_contract_processed(contract_id: str) -> dict:
    """
    Mark a contract as processed using the Tabs API.
    
    Args:
        contract_id: The contract ID to mark as processed
        
    Returns:
        dict: Result with success status and message
    """
    url = f"https://integrators.prod.api.tabsplatform.com/v3/contracts/{contract_id}/actions"
    headers = {
        "Authorization": f"{st.session_state['tabs_api_key']}",
        "Content-Type": "application/json"
    }
    payload = {
        "action": "MARK_AS_PROCESSED"
    }
    
    try:
        print(f"\n=== DEBUG: Mark Contract Processed ===")
        print(f"URL: {url}")
        print(f"Payload: {payload}")
        
        response = requests.post(url, headers=headers, json=payload)
        
        print(f"Response Status: {response.status_code}")
        print(f"Response Body: {response.text[:500] if response.text else 'Empty'}")
        print("=" * 40)
        
        if response.status_code in [200, 201]:
            return {
                "success": True,
                "message": f"Contract {contract_id} marked as processed",
                "response": response.json() if response.text else {}
            }
        else:
            return {
                "success": False,
                "message": f"Failed to mark contract as processed: {response.status_code}",
                "error": response.text
            }
    except Exception as e:
        print(f"Exception in mark_contract_processed: {str(e)}")
        return {
            "success": False,
            "message": f"Error marking contract as processed: {str(e)}"
        }


def get_invoices(customer_id: str, contract_id: str) -> dict:
    """
    Get invoices from Tabs API filtered by customer ID and contract ID.
    
    Args:
        customer_id: The Tabs customer ID
        contract_id: The contract ID
        
    Returns:
        dict: Result with success status, invoices data, and total amount
    """
    # Build filter string with AND logic using comma-separated filters
    filter_str = f'customerId:eq:"{customer_id}",contractId:eq:"{contract_id}"'
    url = f"https://integrators.prod.api.tabsplatform.com/v3/invoices?filter={filter_str}"
    
    headers = {
        "Authorization": f"{st.session_state['tabs_api_key']}"
    }
    
    try:
        print(f"\n=== DEBUG: Get Invoices ===")
        print(f"URL: {url}")
        
        response = requests.get(url, headers=headers)
        
        print(f"Response Status: {response.status_code}")
        print(f"Response Body: {response.text[:500] if response.text else 'Empty'}")
        print("=" * 40)
        
        if response.status_code == 200:
            response_data = response.json()
            invoices = response_data.get("payload", {}).get("data", [])
            
            # Calculate total from all invoices
            total_amount = 0
            for invoice in invoices:
                total_amount += float(invoice.get("total", 0))
            
            return {
                "success": True,
                "message": f"Found {len(invoices)} invoice(s)",
                "invoices": invoices,
                "total_amount": total_amount
            }
        else:
            return {
                "success": False,
                "message": f"Failed to get invoices: {response.status_code}",
                "error": response.text,
                "invoices": [],
                "total_amount": 0
            }
    except Exception as e:
        print(f"Exception in get_invoices: {str(e)}")
        return {
            "success": False,
            "message": f"Error getting invoices: {str(e)}",
            "invoices": [],
            "total_amount": 0
        }


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


# ============================================================
# Tabs Usage API (Beta) - Usage Events
# Reference: https://docs.tabsplatform.com/reference/ingestevent
# ============================================================

USAGE_EVENTS_API_URL = "https://usage-events.prod.api.tabsplatform.com/v1/events"


def create_usage_event(event_data: dict, idempotency_key: str = None) -> dict:
    """
    Create a single usage event using the Tabs Usage API (Beta).
    
    Args:
        event_data: Dictionary containing event data with keys:
            - customer_id: Tabs customer ID
            - event_type_id: Event type ID (from event_to_track)
            - datetime: Event datetime (ISO format or YYYY-MM-DD)
            - value: Numeric value for the event
            - differentiator: Optional differentiator string
            - invoice_split_key: Optional key for invoice splitting
        idempotency_key: Optional unique key for idempotency (auto-generated if not provided)
    
    Returns:
        dict: Response with success status and message
    """
    try:
        # Generate idempotency key if not provided
        if idempotency_key is None:
            idempotency_key = str(uuid.uuid4())
        
        headers = {
            "Authorization": f"{st.session_state['tabs_api_key']}",
            "Content-Type": "application/json"
        }
        
        # Build the event payload
        # Convert datetime to ISO 8601 format if it's just a date
        datetime_value = event_data.get('datetime', '')
        if datetime_value and 'T' not in str(datetime_value):
            datetime_value = f"{datetime_value}T07:00:00Z"
        
        payload = {
            "customerId": event_data.get('customer_id'),
            "eventTypeId": event_data.get('event_type_id'),
            "datetime": datetime_value,
            "value": event_data.get('value'),
            "differentiator": event_data.get('differentiator', ''),
            "idempotencyKey": idempotency_key,
            "invoiceSplitKey": event_data.get('invoice_split_key', '')
        }
        
        # Debug: Print request details
        print(f"\n=== DEBUG: Usage Event API Request ===")
        print(f"URL: {USAGE_EVENTS_API_URL}")
        print(f"Payload: {payload}")
        
        response = requests.post(USAGE_EVENTS_API_URL, headers=headers, json=payload)
        
        # Debug: Print response details
        print(f"Response Status: {response.status_code}")
        print(f"Response Body: {response.text[:500] if response.text else 'Empty'}")
        print("=" * 40)
        
        if response.status_code in [200, 201]:
            return {
                "success": True,
                "message": "Event created successfully",
                "response": response.json() if response.text else {}
            }
        else:
            return {
                "success": False,
                "message": f"Failed to create event: {response.status_code}",
                "error": response.text
            }
            
    except Exception as e:
        print(f"Exception in create_usage_event: {str(e)}")
        return {
            "success": False,
            "message": f"Error creating usage event: {str(e)}"
        }


def create_usage_events_bulk(events: list) -> dict:
    """
    Create multiple usage events in bulk using the Tabs Usage API (Beta).
    Each event is sent individually with a unique idempotency key.
    
    Args:
        events: List of event dictionaries, each containing:
            - customer_id: Tabs customer ID
            - event_type_name: Name of the event type (SKU)
            - datetime: Event datetime (ISO format or YYYY-MM-DD)
            - value: Numeric value for the event
            - differentiator: Optional differentiator string
    
    Returns:
        dict: Summary of results with success/failure counts
    """
    results = {
        "total": len(events),
        "success_count": 0,
        "failure_count": 0,
        "successes": [],
        "failures": []
    }
    
    for i, event_data in enumerate(events):
        # Generate unique idempotency key for each event
        idempotency_key = str(uuid.uuid4())
        
        result = create_usage_event(event_data, idempotency_key)
        
        if result.get("success"):
            results["success_count"] += 1
            results["successes"].append({
                "index": i,
                "customer_id": event_data.get('customer_id'),
                "event_type_name": event_data.get('event_type_name')
            })
        else:
            results["failure_count"] += 1
            results["failures"].append({
                "index": i,
                "customer_id": event_data.get('customer_id'),
                "event_type_name": event_data.get('event_type_name'),
                "error": result.get("message")
            })
    
    return results