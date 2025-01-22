import requests
import sqlite3
import time

#Google Places API Key
API_KEY = "AIzaSyCPiWS3V2wmuN10Z8cRaJsbiz6zbMt40as"

def initialize_database():
    conn = sqlite3.connect("churches.db")  # Create or connect to a database file
    cursor = conn.cursor()
    
    # Correct SQL syntax for table creation
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS churches (
            id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            latitude REAL,
            longitude REAL,
            rating REAL,
            user_ratings_total INTEGER,
            phone_number TEXT,
            website TEXT,
            denomination TEXT DEFAULT 'Unknown',
            worship_style TEXT DEFAULT 'Unknown',
            language TEXT DEFAULT 'Unknown',
            size TEXT DEFAULT 'Unknown'
        )
    ''')
    
    conn.commit()
    return conn

def fetch_churches(location, radius=5000, pagetoken=None):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": location,
        "radius": radius,
        "type": "church",
        "key": API_KEY
    }
    if pagetoken:
        params["pagetoken"] = pagetoken  # Correct way to handle pagination

    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:
        print("Rate limit exceeded. Waiting for 1 minute...")
        time.sleep(60)  # Handle rate limits gracefully
        return fetch_churches(location, radius, pagetoken)  # Retry request
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def fetch_place_details(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,website,formatted_phone_number,types",
        "key": API_KEY
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json().get("result", {})  # Return the detailed result
    else:
        print(f"Error fetching details for {place_id}: {response.status_code}")
        return {}


def save_churches(data, conn):
    cursor = conn.cursor()

    for result in data.get("results", []):
        if not result.get("name") or not result.get("vicinity"):
            continue  # Skip if name or address is missing

        place_id = result.get("place_id")
        place_details = fetch_place_details(place_id)  # Fetch more details
        
        # Ensure default values if Place Details API doesn't return them
        phone_number = place_details.get("formatted_phone_number", "Unknown")
        website = place_details.get("website", "Unknown")  # Fix: Handle missing website

        print(f"Inserting: {result.get('name')} - Phone: {phone_number}, Website: {website}")  # Debugging

        try:
            cursor.execute('''
                INSERT OR IGNORE INTO churches (
                    id, name, address, latitude, longitude, rating, user_ratings_total, phone_number, website, denomination, worship_style, language, size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                place_id,
                result.get("name"),
                result.get("vicinity"),
                result["geometry"]["location"]["lat"],
                result["geometry"]["location"]["lng"],
                result.get("rating"),
                result.get("user_ratings_total"),
                phone_number,  # Ensures this field is always present
                website,  # Ensures this field is always present
                "Unknown",  # Default denomination
                "Unknown",  # Default worship style
                "Unknown",  # Default language
                "Unknown"   # Default size
            ))
        except Exception as e:
            print(f"Error inserting data: {e}")
    
    conn.commit()

def main():
    # Define multiple locations within Cleveland & Chattanooga to cover more ground
    locations = [
        "29.77536689579127, -95.36513867738242",  #Houston, TX
        "30.316078917259667, -97.74491202525337", #Austin, TX
        "29.434792281560405, -98.48994240526817",#San Antonio, TX
        "32.76875578048787, -96.80420329038995", #Dallas, TX
    ]

    conn = initialize_database()

    for city_location in locations:
        print(f"Fetching churches near {city_location}...")
        data = fetch_churches(city_location, radius=5000)  # Smaller radius to avoid hitting the 60-limit

        if data:
            save_churches(data, conn)

            # Handle pagination for more results
            next_page_token = data.get("next_page_token")
            while next_page_token:
                print("Fetching next page...")
                time.sleep(2)  # Required delay
                data = fetch_churches(city_location, radius=5000, pagetoken=next_page_token)
                save_churches(data, conn)
                next_page_token = data.get("next_page_token")

    conn.close()


if __name__ == "__main__":
    main()