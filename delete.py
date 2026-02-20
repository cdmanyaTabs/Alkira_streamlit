"""
Script to retrieve and delete usage events from Tabs Platform API.

This script:
1. Fetches all usage events starting from a specified date
2. Extracts idempotency keys from the events
3. Displays a summary and asks for confirmation
4. Deletes the events using their idempotency keys

Usage:
    python delete.py --dry-run                    # See what would be deleted
    python delete.py --auto-confirm               # Delete without confirmation
    python delete.py --start-date 2026-02-01      # Custom start date
    python delete.py                              # Interactive mode
"""

import requests
import json
import argparse
from typing import List, Dict, Optional
from datetime import datetime

# Configuration
API_BASE_URL = "https://usage-events.prod.api.tabsplatform.com/v1/events"
API_KEY = "tabs_sk_0aaU5BffnzKy0W7hV8ptRaY9OleO4vkc9RtsukuUnSvjjaMmhHbHbYiM84DNyIT8"

# Color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def get_events(start_date: str, api_key: str, debug: bool = False) -> tuple[List[Dict], Optional[str]]:
    """
    Retrieve all usage events starting from the specified date.
    Uses pagination to fetch all events, not just the first page.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        api_key: API key for authentication
        debug: If True, print debug information about API responses
        
    Returns:
        Tuple of (list of events, error message if failed)
    """
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }
    
    all_events = []
    
    # Try different query parameter names since the exact parameter isn't documented
    param_names = ['startDate', 'start_date', 'fromDate', 'datetime_from', 'from']
    
    for param_name in param_names:
        try:
            print(f"Attempting to fetch events with parameter '{param_name}'...")
            
            # Try with a high limit first to minimize API calls
            # We'll use offset-based pagination
            limit = 1000
            offset = 0
            page_num = 1
            
            while True:
                params = {
                    param_name: start_date,
                    'limit': limit,
                    'offset': offset
                }
                
                response = requests.get(API_BASE_URL, headers=headers, params=params, timeout=30)
                
                if page_num == 1:
                    print(f"  Response Status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Debug: Print response structure on first page
                    if debug and page_num == 1:
                        print(f"  Response keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
                        if isinstance(data, dict):
                            total = data.get('total', data.get('count', data.get('totalCount', 'N/A')))
                            print(f"  Total/Count fields: {total}")
                            if 'pagination' in data:
                                print(f"  Pagination: {data.get('pagination')}")
                    
                    # Handle different response structures
                    events = []
                    if isinstance(data, dict):
                        # Check for common pagination structures
                        events = (
                            data.get('events', []) or
                            data.get('data', []) or
                            data.get('payload', {}).get('data', []) or
                            data.get('items', []) or
                            []
                        )
                        
                        # If data itself is a list
                        if not events and isinstance(data, dict) and len(data) == 0:
                            events = []
                        elif not events:
                            # Maybe the whole response is the events array wrapped differently
                            for key, value in data.items():
                                if isinstance(value, list) and len(value) > 0:
                                    events = value
                                    break
                                    
                    elif isinstance(data, list):
                        events = data
                    
                    if not events:
                        # No more events
                        if page_num == 1:
                            # First page is empty, might be wrong parameter
                            break
                        else:
                            # We've fetched all pages
                            print(f"{Colors.OKGREEN}  Successfully fetched {len(all_events)} events total{Colors.ENDC}")
                            return all_events, None
                    
                    events_this_page = len(events)
                    all_events.extend(events)
                    
                    print(f"  Fetched {events_this_page} events (page {page_num}, total so far: {len(all_events)})")
                    
                    # Check if we got fewer events than the limit (means we're done)
                    if events_this_page < limit:
                        print(f"{Colors.OKGREEN}  Successfully fetched {len(all_events)} events total{Colors.ENDC}")
                        return all_events, None
                    
                    # Prepare for next page
                    offset += limit
                    page_num += 1
                    
                elif response.status_code == 401 or response.status_code == 403:
                    error = f"Authentication failed: {response.status_code}"
                    print(f"{Colors.FAIL}  {error}{Colors.ENDC}")
                    return [], error
                    
                elif response.status_code == 400:
                    # Bad request, try next parameter name
                    print(f"  Parameter '{param_name}' not accepted, trying next...")
                    break
                else:
                    print(f"  Unexpected status code: {response.status_code}")
                    print(f"  Response: {response.text[:200]}")
                    break
                    
        except requests.exceptions.RequestException as e:
            error = f"Network error: {str(e)}"
            print(f"{Colors.FAIL}  {error}{Colors.ENDC}")
            return [], error
        except Exception as e:
            error = f"Error fetching events: {str(e)}"
            print(f"{Colors.FAIL}  {error}{Colors.ENDC}")
            return [], error
    
    # If we get here, none of the parameter names worked
    # Try without any date filter
    print(f"\n{Colors.WARNING}Date parameter not working. Attempting to fetch all events (no date filter)...{Colors.ENDC}")
    try:
        limit = 1000
        offset = 0
        page_num = 1
        
        while True:
            params = {'limit': limit, 'offset': offset}
            response = requests.get(API_BASE_URL, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Parse response
                events = []
                if isinstance(data, dict):
                    events = (
                        data.get('events', []) or
                        data.get('data', []) or
                        data.get('payload', {}).get('data', []) or
                        data.get('items', []) or
                        []
                    )
                elif isinstance(data, list):
                    events = data
                
                if not events:
                    if page_num == 1:
                        error = f"Failed to fetch events: No events in response"
                        print(f"{Colors.FAIL}  {error}{Colors.ENDC}")
                        return [], error
                    else:
                        break
                
                events_this_page = len(events)
                all_events.extend(events)
                print(f"  Fetched {events_this_page} events (page {page_num}, total: {len(all_events)})")
                
                if events_this_page < limit:
                    break
                
                offset += limit
                page_num += 1
            else:
                error = f"Failed to fetch events: HTTP {response.status_code}"
                print(f"{Colors.FAIL}  {error}{Colors.ENDC}")
                print(f"  Response: {response.text[:500]}")
                return [], error
        
        print(f"{Colors.WARNING}  Fetched {len(all_events)} events total (no date filter applied){Colors.ENDC}")
        return all_events, None
            
    except Exception as e:
        error = f"Error fetching events: {str(e)}"
        print(f"{Colors.FAIL}  {error}{Colors.ENDC}")
        return [], error


def delete_event(idempotency_key: str, api_key: str) -> tuple[bool, Optional[str]]:
    """
    Delete a single usage event using its idempotency key.
    
    Args:
        idempotency_key: The idempotency key of the event to delete
        api_key: API key for authentication
        
    Returns:
        Tuple of (success boolean, error message if failed)
    """
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }
    
    url = f"{API_BASE_URL}/{idempotency_key}"
    
    try:
        response = requests.delete(url, headers=headers, timeout=30)
        
        if response.status_code in [200, 201, 204]:
            return True, None
        elif response.status_code == 404:
            return False, "Event not found"
        elif response.status_code == 401 or response.status_code == 403:
            return False, f"Authentication failed: {response.status_code}"
        elif response.status_code == 429:
            return False, "Rate limit exceeded"
        else:
            error_msg = f"HTTP {response.status_code}"
            try:
                error_data = response.json()
                error_msg += f": {error_data.get('message', error_data.get('error', response.text[:100]))}"
            except:
                error_msg += f": {response.text[:100]}"
            return False, error_msg
            
    except requests.exceptions.Timeout:
        return False, "Request timeout"
    except requests.exceptions.RequestException as e:
        return False, f"Network error: {str(e)}"
    except Exception as e:
        return False, f"Error: {str(e)}"


def display_event_summary(events: List[Dict], dry_run: bool = False):
    """Display a summary of events to be deleted."""
    print(f"\n{Colors.BOLD}{'='*60}{Colors.ENDC}")
    print(f"{Colors.BOLD}Event Summary{Colors.ENDC}")
    print(f"{Colors.BOLD}{'='*60}{Colors.ENDC}")
    
    if dry_run:
        print(f"\n{Colors.OKCYAN}{Colors.BOLD}=== DRY RUN MODE - NO EVENTS WILL BE DELETED ==={Colors.ENDC}")
    
    print(f"\nTotal events found: {Colors.BOLD}{len(events)}{Colors.ENDC}")
    
    if len(events) == 0:
        return
    
    # Extract sample events (first 5)
    print(f"\n{Colors.BOLD}Sample events:{Colors.ENDC}")
    sample_size = min(5, len(events))
    
    for i, event in enumerate(events[:sample_size], 1):
        # Try to extract relevant info
        idempotency_key = event.get('idempotencyKey', event.get('idempotency_key', 'N/A'))
        customer_id = event.get('customerId', event.get('customer_id', 'N/A'))
        event_date = event.get('datetime', event.get('date', 'N/A'))
        value = event.get('value', 'N/A')
        
        print(f"  {i}. Key: {idempotency_key[:20]}...")
        print(f"     Customer: {customer_id}, Date: {event_date}, Value: {value}")
    
    if len(events) > sample_size:
        print(f"  ... and {len(events) - sample_size} more events")


def main():
    """Main execution function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Delete Tabs usage events from the API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python delete.py --dry-run                    # See what would be deleted
  python delete.py --auto-confirm               # Delete without confirmation
  python delete.py --start-date 2026-02-01      # Custom start date
  python delete.py --debug                      # Show API response details
        """
    )
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be deleted without actually deleting')
    parser.add_argument('--start-date', default='2026-01-01',
                       help='Start date for events (YYYY-MM-DD format, default: 2026-01-01)')
    parser.add_argument('--auto-confirm', action='store_true',
                       help='Skip confirmation prompt and delete immediately')
    parser.add_argument('--debug', action='store_true',
                       help='Show debug information about API responses')
    
    args = parser.parse_args()
    
    print(f"{Colors.HEADER}{Colors.BOLD}")
    print("="*60)
    print("Tabs Usage Events Deletion Tool")
    print("="*60)
    print(f"{Colors.ENDC}")
    
    if args.dry_run:
        print(f"{Colors.OKCYAN}{Colors.BOLD}Running in DRY RUN mode{Colors.ENDC}")
        print(f"No events will actually be deleted.\n")
    
    # Validate API key
    if API_KEY == "YOUR_API_KEY_HERE":
        print(f"{Colors.FAIL}ERROR: Please set your API key in the script!{Colors.ENDC}")
        print("Edit the API_KEY variable at the top of delete.py")
        return
    
    # Step 1: Fetch events
    print(f"\n{Colors.OKCYAN}Step 1: Fetching events from {args.start_date}...{Colors.ENDC}")
    events, error = get_events(args.start_date, API_KEY, debug=args.debug)
    
    if error:
        print(f"\n{Colors.FAIL}Failed to fetch events: {error}{Colors.ENDC}")
        return
    
    if len(events) == 0:
        print(f"\n{Colors.WARNING}No events found to delete.{Colors.ENDC}")
        return
    
    # Step 2: Display summary
    display_event_summary(events, dry_run=args.dry_run)
    
    # Step 3: Get confirmation (skip if auto-confirm or dry-run)
    if not args.auto_confirm and not args.dry_run:
        print(f"\n{Colors.WARNING}{Colors.BOLD}WARNING: This will permanently delete {len(events)} event(s)!{Colors.ENDC}")
        confirmation = input(f"\nType 'yes' to confirm deletion: ").strip()
        
        if confirmation.lower() != 'yes':
            print(f"\n{Colors.WARNING}Deletion cancelled.{Colors.ENDC}")
            return
    elif args.dry_run:
        print(f"\n{Colors.OKCYAN}Dry run complete. No events were deleted.{Colors.ENDC}")
        return
    else:
        print(f"\n{Colors.WARNING}Auto-confirm enabled. Proceeding with deletion...{Colors.ENDC}")
    
    # Step 4: Delete events
    print(f"\n{Colors.OKCYAN}Step 2: Deleting events...{Colors.ENDC}")
    
    deleted_count = 0
    failed_count = 0
    failed_events = []
    
    for i, event in enumerate(events, 1):
        idempotency_key = event.get('idempotencyKey', event.get('idempotency_key', ''))
        
        if not idempotency_key:
            print(f"{Colors.WARNING}  Event {i}/{len(events)}: No idempotency key found, skipping{Colors.ENDC}")
            failed_count += 1
            failed_events.append(("N/A", "No idempotency key"))
            continue
        
        success, error = delete_event(idempotency_key, API_KEY)
        
        if success:
            deleted_count += 1
            if i % 10 == 0 or i == len(events):
                print(f"  Progress: {i}/{len(events)} ({int(i/len(events)*100)}%)")
        else:
            failed_count += 1
            failed_events.append((idempotency_key[:20], error))
            print(f"{Colors.FAIL}  Event {i}/{len(events)}: Failed to delete {idempotency_key[:20]}... - {error}{Colors.ENDC}")
    
    # Step 5: Display final summary
    print(f"\n{Colors.BOLD}{'='*60}{Colors.ENDC}")
    print(f"{Colors.BOLD}Deletion Complete{Colors.ENDC}")
    print(f"{Colors.BOLD}{'='*60}{Colors.ENDC}")
    print(f"\n{Colors.OKGREEN}Successfully deleted: {deleted_count}{Colors.ENDC}")
    print(f"{Colors.FAIL}Failed: {failed_count}{Colors.ENDC}")
    
    if failed_events:
        print(f"\n{Colors.BOLD}Failed events:{Colors.ENDC}")
        for key, error in failed_events[:10]:
            print(f"  - {key}...: {error}")
        if len(failed_events) > 10:
            print(f"  ... and {len(failed_events) - 10} more")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}Operation cancelled by user.{Colors.ENDC}")
    except Exception as e:
        print(f"\n{Colors.FAIL}Unexpected error: {str(e)}{Colors.ENDC}")
        import traceback
        traceback.print_exc()
