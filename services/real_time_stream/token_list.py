import requests


def fetch_data(url):
    """Fetch JSON data from the given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise error for bad responses
        return response.json()
    except Exception as e:
        print(f"Error fetching data from {url}: {e}")
        return {}


# Define API endpoints for different segments.
endpoints = {
    "NSE_CD": "https://public.fyers.in/sym_details/NSE_CD_sym_master.json",  # Currency Derivatives
    "NSE_FO": "https://public.fyers.in/sym_details/NSE_FO_sym_master.json",  # Equity Derivatives
    "NSE_COM": "https://public.fyers.in/sym_details/NSE_COM_sym_master.json",  # Commodity
    "NSE_CM": "https://public.fyers.in/sym_details/NSE_CM_sym_master.json"  # Capital Market
}

# Load data from all endpoints.
data_sources = {}
for segment, url in endpoints.items():
    print(f"Fetching data for {segment} ...")
    data_sources[segment] = fetch_data(url)


def find_instrument_token(search_term):
    """
    Searches all data sources for instruments whose key contains the search_term (case-insensitive).

    Returns a list of dictionaries containing:
      - segment: The segment where the instrument was found.
      - symbol: The full symbol key.
      - fyToken: The instrument token.
      - details: Complete instrument details.
    """
    results = []
    search_term_upper = search_term.upper()
    for segment, data in data_sources.items():
        for key, details in data.items():
            if search_term_upper in key.upper():
                token = details.get("fyToken")
                results.append({
                    "segment": segment,
                    "symbol": key,
                    "fyToken": token,
                    "details": details
                })
    return results


def dynamic_search():
    """
    Continuously prompt the user for a symbol (or part of it) and display matching instrument tokens.

    Type 'q' or press Enter with no input to exit.
    """
    while True:
        search_input = input("\nEnter the symbol to search for (or 'q' to quit): ").strip()
        if not search_input or search_input.lower() == 'q':
            print("Exiting search.")
            break

        matches = find_instrument_token(search_input)
        if matches:
            for match in matches:
                print(f"\nFound in segment: {match['segment']}")
                print(f"Symbol: {match['symbol']}")
                print(f"Instrument Token (fyToken): {match['fyToken']}")
                print("-" * 40)
        else:
            print(f"No matching symbols found for '{search_input}'.")


if __name__ == '__main__':
    dynamic_search()
