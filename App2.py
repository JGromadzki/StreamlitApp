class PropertyFinderScraper:
    def __init__(self, output_file='property_listings_data.csv', checkpoint_file='scraping_checkpoint.json'):
        self.base_url = 'https://www.propertyfinder.ae/en/search?l=1&c=1&fu=0&ob=mr&page={}'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        self.checkpoint_file = checkpoint_file
        self.output_file = output_file
        self.all_listings = []
        self.last_page = 1
        self.max_consecutive_errors = 5
        self.consecutive_errors = 0

    def fetch_listings_from_page(self, page_number):
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    self.base_url.format(page_number), 
                    headers=self.headers,
                    timeout=30
                )
                
                if response.status_code != 200:
                    raise requests.RequestException(f"Status code: {response.status_code}")

                soup = BeautifulSoup(response.content, "html.parser")
                next_data_script = soup.find("script", {"id": "__NEXT_DATA__"})

                if not next_data_script:
                    raise ValueError("No __NEXT_DATA__ script found")

                json_content = next_data_script.string
                data = json.loads(json_content)
                listings = data["props"]["pageProps"]["searchResult"]["listings"]

                if listings:
                    self.consecutive_errors = 0
                    return listings
                else:
                    raise ValueError("No listings found in the response")

            except Exception as e:
                if attempt < max_retries - 1:
                    st.warning(f"Error on page {page_number}, attempt {attempt + 1}/{max_retries}: {str(e)}")
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    self.consecutive_errors += 1
                    st.error(f"Failed to fetch page {page_number} after {max_retries} attempts: {str(e)}")
                    return None

    def process_listings_to_dataframe(self, listings):
        def flatten_dict(d, parent_key='', sep='_'):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    items.append((new_key, str(v) if v and isinstance(v[0], dict) else v))
                else:
                    items.append((new_key, v))
            return dict(items)

        processed_listings = []
        for listing in listings:
            try:
                flat_listing = flatten_dict(listing)
                processed_listings.append(flat_listing)
            except Exception as e:
                st.warning(f"Error processing listing: {str(e)}")

        df = pd.DataFrame(processed_listings)
        df = df.replace({np.nan: None})
        return df

def main():
    st.title("PropertyFinder.ae Web Scraper")
    
    # Initialize session state for tracking scraping progress
    if 'scraper' not in st.session_state:
        st.session_state.scraper = PropertyFinderScraper()
        st.session_state.scraped_data = None
    
    # Scrape Parameters
    max_pages = st.number_input("Maximum Pages to Scrape", min_value=1, max_value=1000, value=10)
    
    # Scrape Button
    if st.button("Start Scraping"):
        # Reset previous data
        st.session_state.scraper.all_listings = []
        
        # Progress bar
        progress_bar = st.progress(0)
        
        try:
            for page_number in range(1, max_pages + 1):
                st.write(f"Fetching page {page_number}...")
                
                listings = st.session_state.scraper.fetch_listings_from_page(page_number)
                
                if not listings:
                    st.info("No more listings found or reached the end")
                    break
                
                st.session_state.scraper.all_listings.extend(listings)
                
                # Update progress
                progress_bar.progress(page_number / max_pages)
                time.sleep(1)  # Polite delay
            
            # Convert to DataFrame
            st.session_state.scraped_data = st.session_state.scraper.process_listings_to_dataframe(
                st.session_state.scraper.all_listings
            )
            
            st.success(f"Scraped {len(st.session_state.scraped_data)} listings!")
        
        except Exception as e:
            st.error(f"Scraping error: {str(e)}")
    
    # Display and Download Data
    if st.session_state.scraped_data is not None:
        st.subheader("Scraped Listings")
        st.dataframe(st.session_state.scraped_data)
        
        # Download Button
        csv = st.session_state.scraped_data.to_csv(index=False)
        st.download_button(
            label="Download Listings as CSV", 
            data=csv, 
            file_name="property_finder_listings.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
