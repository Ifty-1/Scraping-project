import requests
import json
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import argparse
import csv
import os
import pandas as pd
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Common HTTP and Network Functions ---

def create_session():
    """Create a session with retry strategy and return it"""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    # Mount the adapter to the session
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    
    return session

def get_default_headers():
    """Return default headers for HTTP requests"""
    return {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "sec-ch-ua": "\"Not;A=Brand\";v=\"99\", \"Google Chrome\";v=\"139\", \"Chromium\";v=\"139\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }

def add_delay(min_seconds=1, max_seconds=3):
    """Add a random delay to make requests appear more human-like"""
    delay = random.uniform(min_seconds, max_seconds)
    logger.info(f"Waiting for {delay:.2f} seconds...")
    time.sleep(delay)

# --- Generic File Operations ---

def save_json_file(data, filename=None):
    """Save data to a JSON file"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Data successfully saved to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving data to file: {str(e)}")
        return False

# --- Vehicle Search Utilities ---

def build_fallback_search_params(csv_row, dealer_id="12751", source=None):
    """
    Build search parameters for fallback search using vehicle details from CSV
    
    Args:
        csv_row: Dictionary containing vehicle data from CSV
        dealer_id: Dealer ID to filter by
        source: API source ('CG' for Carsguide, None for AutoTrader)
        
    Returns:
        dict: Parameters for the API search
    """
    params = {
        "dealer_id": dealer_id,
        "ipLookup": "1",
        "sorting_variation": "smart_sort_3",
        "paginate": "26"
    }
    
    # Add source if specified (for Carsguide)
    if source:
        params["source"] = source
    
    # Map CSV fields to API parameters
    if "Make" in csv_row and csv_row["Make"] and not pd.isna(csv_row["Make"]):
        params["make"] = str(csv_row["Make"]).strip()
    
    if "Model" in csv_row and csv_row["Model"] and not pd.isna(csv_row["Model"]):
        params["model"] = str(csv_row["Model"]).strip()
    
    if "Year" in csv_row and csv_row["Year"] and not pd.isna(csv_row["Year"]):
        params["manu_year"] = str(csv_row["Year"]).strip()
    
    # Price range: CSV Price ± 100
    if "Price" in csv_row and csv_row["Price"] and not pd.isna(csv_row["Price"]):
        try:
            price = int(str(csv_row["Price"]).strip().replace(',', ''))
            params["priceFrom"] = str(price - 100)
            params["priceTo"] = str(price + 100)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse price: {csv_row.get('Price')}")
    
    # Odometer range: CSV KM ± 100
    if "KM" in csv_row and csv_row["KM"] and not pd.isna(csv_row["KM"]):
        try:
            km = int(str(csv_row["KM"]).strip().replace(',', ''))
            params["odometerFrom"] = str(km - 100)
            params["odometerTo"] = str(km + 100)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse odometer: {csv_row.get('KM')}")
    
    return params

def extract_vehicle_url(vehicle_data, site_type):
    """
    Extract and format vehicle URL for the specified site
    
    Args:
        vehicle_data: Vehicle data from API response
        site_type: 'autotrader' or 'carsguide'
        
    Returns:
        str: Complete URL or empty string if not found
    """
    if not vehicle_data:
        return ""
    
    # Get URL path from vehicle data
    url_path = vehicle_data.get('url', '') or vehicle_data.get('url_cg', '')
    
    if not url_path:
        return ""
    
    # Build complete URL based on site type
    if site_type == 'autotrader':
        return f"https://www.autotrader.com.au/{url_path}"
    elif site_type == 'carsguide':
        return f"https://www.carsguide.com.au/{url_path}"
    else:
        return url_path

# --- AutoTrader API Specific Functions ---

def autotrader_get_cookies(session):
    """Visit AutoTrader website to get cookies"""
    try:
        logger.info("Getting initial cookies from AutoTrader website...")
        main_url = "https://www.autotrader.com.au/"
        response = session.get(
            main_url, 
            headers=get_default_headers(),
            timeout=30
        )
        logger.info(f"Initial website visit status code: {response.status_code}")
        
        if response.status_code == 200:
            logger.info("Successfully obtained initial cookies")
            return True
        else:
            logger.error(f"Failed to get initial cookies, status code: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error getting initial cookies: {str(e)}")
        return False

def autotrader_search_vehicle(session, stock_no, dealer_id="12751", csv_row=None):
    """
    Search for a specific vehicle on AutoTrader by stock number, with fallback search
    
    Args:
        session: Requests session object
        stock_no: Stock number to search for
        dealer_id: Dealer ID to filter by
        csv_row: Dictionary containing CSV row data for fallback search
        
    Returns:
        dict: API response or None if request failed
    """
    # First get cookies
    if not autotrader_get_cookies(session):
        logger.error("Failed to get AutoTrader cookies, aborting search")
        return None
    
    # Add delay
    add_delay()
    
    # Build the API URL
    base_url = "https://listings.platform.autotrader.com.au/api/v3/search"
    
    # First try with stock number
    params = {
        "stock_no": stock_no,
        "dealer_id": dealer_id
    }
    
    try:
        logger.info(f"Sending AutoTrader API request (primary) with params: {params}")
        # Add referer header for legitimacy
        headers = get_default_headers()
        headers["referer"] = "https://www.autotrader.com.au/cars/search"
        
        response = session.get(
            base_url,
            params=params,
            headers=headers,
            timeout=30
        )
        
        logger.info(f"AutoTrader API response status code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            # Check if we found any results
            if result.get('data') and len(result['data']) > 0:
                logger.info("Vehicle found with stock number search")
                return result
            else:
                logger.info("No results found with stock number, trying fallback search...")
                
                # Try fallback search if csv_row is provided
                if csv_row:
                    add_delay(1, 2)  # Add delay before fallback search
                    
                    # Build fallback search parameters
                    fallback_params = build_fallback_search_params(csv_row, dealer_id)
                    
                    logger.info(f"Sending AutoTrader API request (fallback) with params: {fallback_params}")
                    
                    fallback_response = session.get(
                        base_url,
                        params=fallback_params,
                        headers=headers,
                        timeout=30
                    )
                    
                    if fallback_response.status_code == 200:
                        fallback_result = fallback_response.json()
                        if fallback_result.get('data') and len(fallback_result['data']) > 0:
                            logger.info("Vehicle found with fallback search parameters")
                            return fallback_result
                        else:
                            logger.info("No results found even with fallback search")
                    else:
                        logger.error(f"Fallback search failed with status code: {fallback_response.status_code}")
                
                return result  # Return original empty result
                
        elif response.status_code == 403:
            logger.warning("Bot protection detected (403 Forbidden). Retrying with additional measures...")
            
            # Wait longer and try again with a slightly different user agent
            time.sleep(random.uniform(5, 8))
            headers["user-agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0"
            
            # Get fresh cookies
            autotrader_get_cookies(session)
            
            # Try again
            response = session.get(
                base_url,
                params=params,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info("Retry successful!")
                return response.json()
            else:
                logger.error(f"Retry failed with status code: {response.status_code}")
                return None
        else:
            logger.error(f"Request failed with status code: {response.status_code}")
            return None
                
    except Exception as e:
        logger.error(f"Error making AutoTrader API request: {str(e)}")
        return None

def autotrader_extract_vehicle_data(api_data):
    """Extract relevant vehicle data from AutoTrader API response"""
    if not api_data or 'data' not in api_data or not api_data['data']:
        return None
    
    # Get the vehicle data from the first result
    return api_data['data'][0]['_source']

def autotrader_compare_data(csv_data, vehicle_data):
    """
    Compare vehicle data from CSV with AutoTrader API data
    
    Returns:
        tuple: (status, mismatched_fields)
    """
    if not vehicle_data:
        return "Not Found", []
    
    # Check if the vehicle is marked as sold or on offer
    status = vehicle_data.get('status')
    if status and status.lower() != 'live':
        if status.lower() == 'sold':
            return "Sold", []
        elif status.lower() == 'on offer':
            return "On Offer", []
    
    # Fields to compare (CSV field name, API field path)
    comparison_fields = {
        'Fuel': ('vehicle', 'fuel_type'),
        'Model': ('model',),
        'Seats': ('vehicle', 'seats'),
        'Doors': ('vehicle', 'doors'), 
        'Transmission': ('vehicle', 'transmission_type'),
        'Tansmission': ('vehicle', 'transmission_type'),
        'Price': ('price', 'advertised_price')
    }
    
    mismatched = []
    
    # Compare each field if it exists in the CSV data
    for csv_field, api_path in comparison_fields.items():
        if csv_field in csv_data and csv_data[csv_field]:
            # Get API value
            api_value = vehicle_data
            for key in api_path:
                if isinstance(api_value, dict) and key in api_value:
                    api_value = api_value[key]
                else:
                    api_value = None
                    break
            
            # Special handling for price
            if csv_field == 'Price' and api_value is not None and csv_data[csv_field]:
                try:
                    csv_price = int(str(csv_data[csv_field]).strip().replace(',', ''))
                    api_price = int(str(api_value).strip().replace(',', ''))
                    if abs(csv_price - api_price) > 100:  # Allow small price differences (within $100)
                        mismatched.append(f"{csv_field}: CSV={csv_price}, API={api_price}")
                except (ValueError, TypeError):
                    mismatched.append(f"{csv_field}: Couldn't compare")
            
            # Compare other fields (case-insensitive)
            elif api_value is not None:
                csv_value = str(csv_data[csv_field]).strip()
                api_value_str = str(api_value).strip()
                
                if csv_value.lower() != api_value_str.lower():
                    mismatched.append(f"{csv_field}: CSV={csv_value}, API={api_value_str}")
    
    # Determine status based on mismatches
    if mismatched:
        return "Mismatched", mismatched
    else:
        return "Found", []

def autotrader_format_vehicle_details(vehicle_data):
    """Format vehicle details for display"""
    if not vehicle_data:
        return "No vehicle data available"
    
    details = []
    details.append("\n" + "="*60)
    details.append(f"VEHICLE DETAILS - Stock #{vehicle_data.get('stock_no', 'N/A')}".center(60))
    details.append("="*60)
    details.append(f"Make:           {vehicle_data.get('make', 'N/A')}")
    details.append(f"Model:          {vehicle_data.get('model', 'N/A')}")
    details.append(f"Variant:        {vehicle_data.get('variant', 'N/A')}")
    details.append(f"Year:           {vehicle_data.get('manu_year', 'N/A')}")
    details.append(f"Price:          ${vehicle_data.get('price', {}).get('advertised_price', 'N/A'):,}")
    details.append(f"Color:          {vehicle_data.get('colour_body', 'N/A')}")
    details.append(f"Odometer:       {vehicle_data.get('odometer', 'N/A'):,} km")
    details.append(f"Registration:   {vehicle_data.get('rego', 'N/A')}")
    details.append(f"VIN:            {vehicle_data.get('vin', 'N/A')}")
    details.append(f"Location:       {vehicle_data.get('location_city', 'N/A')}, {vehicle_data.get('location_state', 'N/A')}")
    
    # Get vehicle specifications
    vehicle_specs = vehicle_data.get('vehicle', {})
    if vehicle_specs:
        details.append("\n" + "-"*60)
        details.append("SPECIFICATIONS".center(60))
        details.append("-"*60)
        details.append(f"Body Type:      {vehicle_specs.get('body_type', 'N/A')}")
        details.append(f"Transmission:   {vehicle_specs.get('transmission_type', 'N/A')}")
        details.append(f"Fuel Type:      {vehicle_specs.get('fuel_type', 'N/A')}")
        details.append(f"Engine Size:    {vehicle_specs.get('engine_size', 'N/A')} L")
        details.append(f"Cylinders:      {vehicle_specs.get('cylinders', 'N/A')}")
        details.append(f"Drive Type:     {vehicle_specs.get('drive_type', 'N/A')}")
        details.append(f"Seats:          {vehicle_specs.get('seats', 'N/A')}")
        details.append(f"Doors:          {vehicle_specs.get('doors', 'N/A')}")
    
    # Description
    if vehicle_data.get('description'):
        details.append("\n" + "-"*60)
        details.append("DESCRIPTION".center(60))
        details.append("-"*60)
        
        description = vehicle_data.get('description', 'N/A').replace('\\r\\n', '\n').replace('\r\n', '\n')
        max_desc_length = 500
        if len(description) > max_desc_length:
            description = description[:max_desc_length] + "... [Description truncated]"
        details.append(description)
        
    details.append("\n" + "="*60)
    
    return "\n".join(details)

# --- Carsguide API Specific Functions ---

def carsguide_get_cookies(session):
    """Visit Carsguide website to get cookies"""
    try:
        logger.info("Getting initial cookies from Carsguide website...")
        main_url = "https://www.carsguide.com.au/"
        response = session.get(
            main_url, 
            headers=get_default_headers(),
            timeout=30
        )
        logger.info(f"Initial website visit status code: {response.status_code}")
        
        if response.status_code == 200:
            logger.info("Successfully obtained initial cookies")
            return True
        else:
            logger.error(f"Failed to get initial cookies, status code: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error getting initial cookies: {str(e)}")
        return False

def carsguide_search_vehicle(session, stock_no, dealer_id="12751", make=None, csv_row=None):
    """
    Search for a specific vehicle on Carsguide by stock number, with fallback search
    
    Args:
        session: Requests session object
        stock_no: Stock number to search for
        dealer_id: Dealer ID to filter by
        make: Vehicle make (optional, helps with search accuracy)
        csv_row: Dictionary containing CSV row data for fallback search
        
    Returns:
        dict: API response or None if request failed
    """
    # First get cookies
    if not carsguide_get_cookies(session):
        logger.error("Failed to get Carsguide cookies, aborting search")
        return None
    
    # Add delay
    add_delay()
    
    # Build the API URL
    base_url = "https://listings.platform.autotrader.com.au/api/v3/search"
    
    # First try with stock number
    params = {
        "stock_no": stock_no,
        "dealer_id": dealer_id,
        "source": "CG",  # This identifies it as a Carsguide request
        "ipLookup": "1",
        "sorting_variation": "smart_sort_3",
        "paginate": "26"
    }
    
    # Add make parameter if provided
    if make:
        params["make"] = make
    
    try:
        logger.info(f"Sending Carsguide API request (primary) with params: {params}")
        # Add referer header for Carsguide
        headers = get_default_headers()
        headers["referrer"] = "https://www.carsguide.com.au/"
        headers["sec-fetch-site"] = "cross-site"
        
        response = session.get(
            base_url,
            params=params,
            headers=headers,
            timeout=30
        )
        
        logger.info(f"Carsguide API response status code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            # Check if we found any results
            if result.get('data') and len(result['data']) > 0:
                logger.info("Vehicle found with stock number search")
                return result
            else:
                logger.info("No results found with stock number, trying fallback search...")
                
                # Try fallback search if csv_row is provided
                if csv_row:
                    add_delay(1, 2)  # Add delay before fallback search
                    
                    # Build fallback search parameters with source=CG
                    fallback_params = build_fallback_search_params(csv_row, dealer_id, source="CG")
                    
                    logger.info(f"Sending Carsguide API request (fallback) with params: {fallback_params}")
                    
                    fallback_response = session.get(
                        base_url,
                        params=fallback_params,
                        headers=headers,
                        timeout=30
                    )
                    
                    if fallback_response.status_code == 200:
                        fallback_result = fallback_response.json()
                        if fallback_result.get('data') and len(fallback_result['data']) > 0:
                            logger.info("Vehicle found with fallback search parameters")
                            return fallback_result
                        else:
                            logger.info("No results found even with fallback search")
                    else:
                        logger.error(f"Fallback search failed with status code: {fallback_response.status_code}")
                
                return result  # Return original empty result
                
        elif response.status_code == 403:
            logger.warning("Bot protection detected (403 Forbidden). Retrying with additional measures...")
            
            # Wait longer and try again with a slightly different user agent
            time.sleep(random.uniform(5, 8))
            headers["user-agent"] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
            
            # Get fresh cookies
            carsguide_get_cookies(session)
            
            # Try again
            response = session.get(
                base_url,
                params=params,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info("Retry successful!")
                return response.json()
            else:
                logger.error(f"Retry failed with status code: {response.status_code}")
                return None
        else:
            logger.error(f"Request failed with status code: {response.status_code}")
            return None
                
    except Exception as e:
        logger.error(f"Error making Carsguide API request: {str(e)}")
        return None

def carsguide_extract_vehicle_data(api_data):
    """Extract relevant vehicle data from Carsguide API response"""
    if not api_data or 'data' not in api_data or not api_data['data']:
        return None
    
    # Get the vehicle data from the first result
    return api_data['data'][0]['_source']

def carsguide_compare_data(csv_data, vehicle_data):
    """
    Compare vehicle data from CSV with Carsguide API data
    
    Returns:
        tuple: (status, mismatched_fields)
    """
    if not vehicle_data:
        return "Not Found", []
    
    # Check if the vehicle is marked as sold or on offer
    status = vehicle_data.get('status')
    if status and status.lower() != 'live':
        if status.lower() == 'sold':
            return "Sold", []
        elif status.lower() == 'on offer':
            return "On Offer", []
    
    # Fields to compare (CSV field name, API field path) - same structure as AutoTrader
    comparison_fields = {
        'Fuel': ('vehicle', 'fuel_type'),
        'Model': ('model',),
        'Seats': ('vehicle', 'seats'),
        'Doors': ('vehicle', 'doors'), 
        'Transmission': ('vehicle', 'transmission_type'),
        'Tansmission': ('vehicle', 'transmission_type'),
        'Price': ('price', 'advertised_price')
    }
    
    mismatched = []
    
    # Compare each field if it exists in the CSV data
    for csv_field, api_path in comparison_fields.items():
        if csv_field in csv_data and csv_data[csv_field]:
            # Get API value
            api_value = vehicle_data
            for key in api_path:
                if isinstance(api_value, dict) and key in api_value:
                    api_value = api_value[key]
                else:
                    api_value = None
                    break
            
            # Special handling for price
            if csv_field == 'Price' and api_value is not None and csv_data[csv_field]:
                try:
                    csv_price = int(str(csv_data[csv_field]).strip().replace(',', ''))
                    api_price = int(str(api_value).strip().replace(',', ''))
                    if abs(csv_price - api_price) > 100:  # Allow $100 difference
                        mismatched.append(f"{csv_field}: CSV={csv_price}, API={api_price}")
                except (ValueError, TypeError):
                    mismatched.append(f"{csv_field}: Couldn't compare")
            
            # Compare other fields (case-insensitive)
            elif api_value is not None:
                csv_value = str(csv_data[csv_field]).strip()
                api_value_str = str(api_value).strip()
                
                if csv_value.lower() != api_value_str.lower():
                    mismatched.append(f"{csv_field}: CSV={csv_value}, API={api_value_str}")
    
    # Return status
    if mismatched:
        return "Mismatched", mismatched
    else:
        return "Found", []

def carsguide_format_vehicle_details(vehicle_data):
    """Format vehicle details for display"""
    if not vehicle_data:
        return "No vehicle data available"
    
    details = []
    details.append("\n" + "="*60)
    details.append(f"CARSGUIDE VEHICLE DETAILS - Stock #{vehicle_data.get('stock_no', 'N/A')}".center(60))
    details.append("="*60)
    details.append(f"Make:           {vehicle_data.get('make', 'N/A')}")
    details.append(f"Model:          {vehicle_data.get('model', 'N/A')}")
    details.append(f"Variant:        {vehicle_data.get('variant', 'N/A')}")
    details.append(f"Year:           {vehicle_data.get('manu_year', 'N/A')}")
    details.append(f"Price:          ${vehicle_data.get('price', {}).get('advertised_price', 'N/A'):,}")
    details.append(f"Color:          {vehicle_data.get('colour_body', 'N/A')}")
    details.append(f"Odometer:       {vehicle_data.get('odometer', 'N/A'):,} km")
    details.append(f"Registration:   {vehicle_data.get('rego', 'N/A')}")
    details.append(f"VIN:            {vehicle_data.get('vin', 'N/A')}")
    details.append(f"Location:       {vehicle_data.get('location_city', 'N/A')}, {vehicle_data.get('location_state', 'N/A')}")
    
    # Get vehicle specifications
    vehicle_specs = vehicle_data.get('vehicle', {})
    if vehicle_specs:
        details.append("\n" + "-"*60)
        details.append("SPECIFICATIONS".center(60))
        details.append("-"*60)
        details.append(f"Body Type:      {vehicle_specs.get('body_type', 'N/A')}")
        details.append(f"Transmission:   {vehicle_specs.get('transmission_type', 'N/A')}")
        details.append(f"Fuel Type:      {vehicle_specs.get('fuel_type', 'N/A')}")
        details.append(f"Engine Size:    {vehicle_specs.get('engine_size', 'N/A')} L")
        details.append(f"Cylinders:      {vehicle_specs.get('cylinders', 'N/A')}")
        details.append(f"Drive Type:     {vehicle_specs.get('drive_type', 'N/A')}")
        details.append(f"Seats:          {vehicle_specs.get('seats', 'N/A')}")
        details.append(f"Doors:          {vehicle_specs.get('doors', 'N/A')}")
    
    # Description
    if vehicle_data.get('description'):
        details.append("\n" + "-"*60)
        details.append("DESCRIPTION".center(60))
        details.append("-"*60)
        
        description = vehicle_data.get('description', 'N/A').replace('\\r\\n', '\n').replace('\r\n', '\n')
        max_desc_length = 500
        if len(description) > max_desc_length:
            description = description[:max_desc_length] + "... [Description truncated]"
        details.append(description)
        
    details.append("\n" + "="*60)
    
    return "\n".join(details)

# --- CSV Processing Functions ---

def process_csv_file(csv_path, output_path, save_results=False, verbose=False):
    """Process CSV file and compare with vehicle data from different APIs"""
    print(f"Starting CSV processing for: {csv_path}")
    print(f"Output will be saved to: {output_path}")
    
    # Configure logging level
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize session
    session = create_session()
    
    # Load the CSV file
    try:
        print(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        print(f"Successfully loaded CSV with {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
        
        # Print first few rows for debugging
        print("\nFirst few rows of CSV data:")
        print(df.head().to_string())
    except Exception as e:
        print(f"Error loading CSV file: {str(e)}")
        logger.error(f"Error loading CSV file: {str(e)}")
        return
    
    # Add output columns if they don't exist
    if 'Autotrader' not in df.columns:
        df['Autotrader'] = ''
    if 'Autotrader Notes' not in df.columns:
        df['Autotrader Notes'] = ''
    if 'Autotrader URL' not in df.columns:
        df['Autotrader URL'] = ''
    if 'Carsguide' not in df.columns:
        df['Carsguide'] = ''
    if 'Carsguide Notes' not in df.columns:
        df['Carsguide Notes'] = ''
    if 'Carsguide URL' not in df.columns:
        df['Carsguide URL'] = ''
    
    # Process each row
    for index, row in df.iterrows():
        try:
            # Concatenate Year and StockNo to get the stock number
            year = str(row.get('Year', ''))
            stock_no = str(row.get('StockNo', ''))
            make = str(row.get('Make', ''))
            
            if not year or not stock_no or pd.isna(year) or pd.isna(stock_no):
                logger.warning(f"Row {index+1}: Missing Year or StockNo, skipping")
                df.at[index, 'Autotrader'] = 'Not Searched'
                df.at[index, 'Autotrader Notes'] = 'Missing Year or StockNo'
                df.at[index, 'Autotrader URL'] = ''
                df.at[index, 'Carsguide'] = 'Not Searched'
                df.at[index, 'Carsguide Notes'] = 'Missing Year or StockNo'
                df.at[index, 'Carsguide URL'] = ''
                continue
            
            # Create the combined stock number
            combined_stock_no = str(year) + str(stock_no)
            
            logger.info(f"Processing row {index+1}: {year} {make} {row.get('Model', '')}, Stock: {combined_stock_no}")
            
            # Default dealer ID
            dealer_id = "12751"
            
            # Convert row to dictionary for fallback search
            car_dict = row.to_dict()
            
            # --- Search AutoTrader ---
            autotrader_result = autotrader_search_vehicle(session, combined_stock_no, dealer_id, csv_row=car_dict)
            
            # Process AutoTrader result
            if autotrader_result and 'data' in autotrader_result:
                if len(autotrader_result['data']) == 0:
                    logger.warning(f"No vehicle found in AutoTrader with stock number: {combined_stock_no}")
                    df.at[index, 'Autotrader'] = 'Not Found'
                    df.at[index, 'Autotrader Notes'] = 'Vehicle not found in AutoTrader API'
                    df.at[index, 'Autotrader URL'] = ''
                else:
                    # Save AutoTrader result if requested
                    if save_results:
                        result_filename = f"autotrader_{combined_stock_no}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        save_json_file(autotrader_result, result_filename)
                    
                    # Extract and compare AutoTrader data
                    autotrader_vehicle_data = autotrader_extract_vehicle_data(autotrader_result)
                    at_status, at_mismatches = autotrader_compare_data(car_dict, autotrader_vehicle_data)
                    
                    # Extract URL for AutoTrader
                    at_url = extract_vehicle_url(autotrader_vehicle_data, 'autotrader')
                    
                    # Update DataFrame with AutoTrader results
                    df.at[index, 'Autotrader'] = at_status
                    df.at[index, 'Autotrader Notes'] = '; '.join(at_mismatches) if at_mismatches else ''
                    df.at[index, 'Autotrader URL'] = at_url
                    
                    logger.info(f"AutoTrader - Row {index+1}: Status={at_status}")
            else:
                logger.error(f"Failed to retrieve AutoTrader data for stock number: {combined_stock_no}")
                df.at[index, 'Autotrader'] = 'API Error'
                df.at[index, 'Autotrader Notes'] = 'Failed to retrieve data from AutoTrader API'
                df.at[index, 'Autotrader URL'] = ''
            
            # Add delay between API calls
            add_delay(1, 2)
            
            # --- Search Carsguide ---
            carsguide_result = carsguide_search_vehicle(session, combined_stock_no, dealer_id, 
                                                       make if make and not pd.isna(make) else None, 
                                                       csv_row=car_dict)
            
            # Process Carsguide result
            if carsguide_result and 'data' in carsguide_result:
                if len(carsguide_result['data']) == 0:
                    logger.warning(f"No vehicle found in Carsguide with stock number: {combined_stock_no}")
                    df.at[index, 'Carsguide'] = 'Not Found'
                    df.at[index, 'Carsguide Notes'] = 'Vehicle not found in Carsguide API'
                    df.at[index, 'Carsguide URL'] = ''
                else:
                    # Save Carsguide result if requested
                    if save_results:
                        result_filename = f"carsguide_{combined_stock_no}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        save_json_file(carsguide_result, result_filename)
                    
                    # Extract and compare Carsguide data
                    carsguide_vehicle_data = carsguide_extract_vehicle_data(carsguide_result)
                    cg_status, cg_mismatches = carsguide_compare_data(car_dict, carsguide_vehicle_data)
                    
                    # Extract URL for Carsguide
                    cg_url = extract_vehicle_url(carsguide_vehicle_data, 'carsguide')
                    
                    # Update DataFrame with Carsguide results
                    df.at[index, 'Carsguide'] = cg_status
                    df.at[index, 'Carsguide Notes'] = '; '.join(cg_mismatches) if cg_mismatches else ''
                    df.at[index, 'Carsguide URL'] = cg_url
                    
                    logger.info(f"Carsguide - Row {index+1}: Status={cg_status}")
            else:
                logger.error(f"Failed to retrieve Carsguide data for stock number: {combined_stock_no}")
                df.at[index, 'Carsguide'] = 'API Error'
                df.at[index, 'Carsguide Notes'] = 'Failed to retrieve data from Carsguide API'
                df.at[index, 'Carsguide URL'] = ''
            
            # Print progress summary
            at_status = df.at[index, 'Autotrader']
            cg_status = df.at[index, 'Carsguide']
            print(f"Processed {index+1}/{len(df)} - {make} {row.get('Model', '')} (Stock: {combined_stock_no}): AT={at_status}, CG={cg_status}")
            
            # Add delay between vehicles
            add_delay(1, 2)
            
        except Exception as e:
            logger.error(f"Error processing row {index+1}: {str(e)}")
            df.at[index, 'Autotrader'] = 'Error'
            df.at[index, 'Autotrader Notes'] = f'Error processing: {str(e)}'
            df.at[index, 'Autotrader URL'] = ''
            df.at[index, 'Carsguide'] = 'Error'
            df.at[index, 'Carsguide Notes'] = f'Error processing: {str(e)}'
            df.at[index, 'Carsguide URL'] = ''
    
    # Save the output CSV
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Results saved to {output_path}")
        print(f"\nResults saved to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error saving output CSV: {str(e)}")
        print(f"Error saving output CSV: {str(e)}")
        return None

def display_summary(output_path):
    """Display a summary of the processed results"""
    try:
        df = pd.read_csv(output_path)
        
        print("\n" + "="*60)
        print("PROCESSING SUMMARY".center(60))
        print("="*60)
        
        print(f"Total vehicles processed: {len(df)}")
        
        # AutoTrader summary
        if 'Autotrader' in df.columns:
            at_status_counts = df['Autotrader'].value_counts().to_dict()
            print(f"\nAutoTrader Results:")
            for status, count in at_status_counts.items():
                print(f"  {status}: {count}")
        
        # Carsguide summary
        if 'Carsguide' in df.columns:
            cg_status_counts = df['Carsguide'].value_counts().to_dict()
            print(f"\nCarsguide Results:")
            for status, count in cg_status_counts.items():
                print(f"  {status}: {count}")
            
        print("\nOutput saved to: " + output_path)
        print("="*60)
    except Exception as e:
        logger.error(f"Error generating summary: {str(e)}")
        print(f"\nError generating summary: {str(e)}")
        print(f"Output saved to: {output_path}")

# --- Main Function ---

def main():
    """Main function to run the scraper"""
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Vehicle Data Scraper')
    parser.add_argument('--csv', type=str, help='Path to input CSV file with vehicle data')
    parser.add_argument('--stock_no', type=str, help='Stock number to search for (single vehicle)')
    parser.add_argument('--api', type=str, choices=['autotrader', 'carsguide', 'both'], default='both', 
                       help='Which API to use for single vehicle search (default: both)')
    parser.add_argument('--make', type=str, help='Vehicle make (helps with Carsguide search accuracy)')
    parser.add_argument('--save', action='store_true', help='Save API results to file')
    parser.add_argument('--output', type=str, help='Output file name (default: processed_cars_<timestamp>.csv)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--dealer_id', type=str, default="12751", help='Dealer ID to use for searches')
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check if we're processing a CSV or a single stock number
    if args.csv:
        # Process the CSV file - always searches both APIs
        output_path = args.output
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"processed_cars_{timestamp}.csv"
        
        print(f"Processing CSV file: {args.csv}")
        print("Searching both AutoTrader and Carsguide for all vehicles...")
        
        output_path = process_csv_file(args.csv, output_path, args.save, args.verbose)
        if output_path:
            display_summary(output_path)
        return
    
    # Single vehicle processing
    if not args.stock_no:
        parser.print_help()
        return
    
    # Initialize session
    session = create_session()
    
    # Get parameters
    stock_no = args.stock_no
    dealer_id = args.dealer_id
    make = args.make
    
    logger.info(f"Searching for stock number: {stock_no} with dealer ID: {dealer_id}")
    
    # Search AutoTrader if requested
    if args.api in ['autotrader', 'both']:
        print(f"\n{'='*60}")
        print("SEARCHING AUTOTRADER")
        print(f"{'='*60}")
        
        autotrader_result = autotrader_search_vehicle(session, stock_no, dealer_id)
        
        if autotrader_result and 'data' in autotrader_result and len(autotrader_result['data']) > 0:
            logger.info("Successfully retrieved AutoTrader vehicle data!")
            
            # Save to file if requested
            if args.save:
                output_name = f"autotrader_data_{stock_no}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                save_json_file(autotrader_result, output_name)
            
            # Extract and format vehicle data
            vehicle_data = autotrader_extract_vehicle_data(autotrader_result)
            vehicle_details = autotrader_format_vehicle_details(vehicle_data)
            print(vehicle_details)
        else:
            logger.error("Failed to retrieve AutoTrader data or no vehicle found")
            print("No vehicle found in AutoTrader or API error occurred")
    
    # Search Carsguide if requested
    if args.api in ['carsguide', 'both']:
        print(f"\n{'='*60}")
        print("SEARCHING CARSGUIDE")
        print(f"{'='*60}")
        
        carsguide_result = carsguide_search_vehicle(session, stock_no, dealer_id, make)
        
        if carsguide_result and 'data' in carsguide_result and len(carsguide_result['data']) > 0:
            logger.info("Successfully retrieved Carsguide vehicle data!")
            
            # Save to file if requested
            if args.save:
                output_name = f"carsguide_data_{stock_no}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                save_json_file(carsguide_result, output_name)
            
            # Extract and format vehicle data
            vehicle_data = carsguide_extract_vehicle_data(carsguide_result)
            vehicle_details = carsguide_format_vehicle_details(vehicle_data)
            print(vehicle_details)
        else:
            logger.error("Failed to retrieve Carsguide data or no vehicle found")
            print("No vehicle found in Carsguide or API error occurred")

if __name__ == "__main__":
    main()
