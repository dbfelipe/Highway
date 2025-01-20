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


def save_churches(data, conn):
    cursor = conn.cursor()
    for result in data.get("results", []):
        if not result.get("name") or not result.get("vicinity"):
            continue  # Skip this result if name or vicinity is missing
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO churches (
                    id, name, address, latitude, longitude, rating, user_ratings_total, denomination, worship_style, language, size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                result.get("place_id"),
                result.get("name"),
                result.get("vicinity"),
                result["geometry"]["location"]["lat"],
                result["geometry"]["location"]["lng"],
                result.get("rating"),
                result.get("user_ratings_total"),
                "Unknown",  # Default denomination
                "Unknown",  # Default worship style
                "Unknown",  # Default language
                "Unknown"   # Default size
))
        except Exception as e:
            print(f"Error inserting data: {e}")
    conn.commit()

def main():
    # City coordinates
    city_location = "35.158985377826724, -84.87618196353363"  #Cleveland, Tennessee
    conn = initialize_database()

    # Fetch data
    data = fetch_churches(city_location)
    if data:
        save_churches(data, conn)

        # Handle pagination if more results are available
        next_page_token = data.get("next_page_token")
        while next_page_token:
            print("Fetching next page...")
            time.sleep(2)  # Google requires a short delay before using the token
            data = fetch_churches(city_location, pagetoken=next_page_token)
            save_churches(data, conn)
            next_page_token = data.get("next_page_token")

    conn.close()

if __name__ == "__main__":
    main()