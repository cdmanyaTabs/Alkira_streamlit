# Alkira Streamlit App


Usage Transformation for Alkira
## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your API key:
   - Option 1: Add it to `.streamlit/secrets.toml`:
   ```toml
   tabs_api_key = "your-api-key-here"
   ```
   - Option 2: Enter it in the sidebar when running the app

3. Run the app:
```bash
streamlit run main.py
```

## API

- Get All Customers by Customer Custom Field - Tenant ID
- Get Integration Items
- Get Events

## Files to Upload:
1. Price Book ZIP
2. Prepaid 
3. Enterprise Support

## Project Structure

- `main.py` - Main Streamlit application
- `api.py` - API functions for Tabs platform integration
- `requirements.txt` - Python dependencies
- `.streamlit/config.toml` - Streamlit configuration
- `.streamlit/secrets.toml` - Secrets (API keys, etc.)


Things to store
CustomerIDs
EventToTrackIDs
IntegrationItemIDs

1. Transform BT sheet
2. Filter which customers / BTs are in Raw Usage
3. Add Enterprise Support + Prepaid
4. Create Contracts
5. Upload BTs
6. Mark Contract as Processed
7. Prep Usage File CSV