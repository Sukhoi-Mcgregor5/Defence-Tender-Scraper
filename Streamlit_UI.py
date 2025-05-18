import streamlit as st
import pandas as pd
import os
import time
import numpy as np
from datetime import datetime, timedelta

# Import scraping functions from the scraper code file
from craper import scrape_isro_data, scrape_invest_india_data, scrape_nal_data, scrape_gem_data, \
    scrape_dst_data, scrape_bdl_data

# Load pre-saved data from Excel or CSV files
def load_data(file_path):
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    else:
        return pd.read_excel(file_path)

# Function to filter data for the last 7 days
def filter_last_7_days(data):
    if 'Scraped Date' in data.columns:
        data['Scraped Date'] = pd.to_datetime(data['Scraped Date'], errors='coerce')
        seven_days_ago = datetime.now() - timedelta(days=7)
        latest_data = data[data['Scraped Date'] >= seven_days_ago]
        return latest_data
    else:
        return pd.DataFrame(columns=data.columns)

# Define file paths for pre-saved data
base_path = r'(enter your path here)'
file_paths = {
    'NAL': os.path.join(base_path, 'nal_tender_data.xlsx'),
    'Invest India': os.path.join(base_path, 'invest_india_data.xlsx'),
    'GEM': os.path.join(base_path, 'scraped_gem_data.xlsx'),
    'ISRO': os.path.join(base_path, 'isro_scraped_data.csv'),
    'DST': os.path.join(base_path, 'scraped_DST.csv'),
    'Srijan': os.path.join(base_path, 'srijan_defence_products_with_dates.xlsx'),
    'BDL': os.path.join(base_path, 'bdl_tenders.csv')  # BDL data path
}

# URLs corresponding to the websites for each dataset
website_urls = {
    'NAL': 'https://www.nal.res.in/en/tender-purchase',
    'Invest India': 'https://www.investindia.gov.in/request-for-proposal',
    'GEM': 'https://mkp.gem.gov.in/',
    'ISRO': 'https://eproc.isro.gov.in/home.html',
    'DST': 'https://dst.gov.in/search/node/grants',
    'Srijan': 'https://srijandefence.gov.in/',
    'BDL': 'https://bdltenders.abcprocure.com/ssoeprocurement/EProc.jsp'  # BDL website
}

# Define the scraping times for each website (in seconds)
scrape_times = {
    "All": 150,
    "GEM": 90,
    "NAL": 10,
    "Invest India": 30,
    "ISRO": 45,
    "DST": 45,
    "Srijan": 60,
    "BDL": 25
}

# Title of the application
st.title("Tender Data Viewer")

# Move the website link to the top, below the title
selected_site = st.selectbox("Choose a website to view data:", list(file_paths.keys()))

if selected_site in website_urls and website_urls[selected_site]:
    st.markdown(f"[Go to {selected_site} Website]({website_urls[selected_site]})", unsafe_allow_html=True)

# Sidebar for rescraping using radio buttons
st.sidebar.title("Rescrape Options")
selected_rescrape = st.sidebar.radio("Choose a website to rescrape:", ['All'] + list(file_paths.keys()))
rescrape_button_sidebar = st.sidebar.button("Rescrape")

# Sidebar placeholder for the scraping message
sidebar_message_placeholder = st.sidebar.empty()

# Search bar above the table
search_term = st.text_input("Search Entries")

# Initialize a placeholder for the progress bar right below the dropdown
progress_placeholder = st.empty()

# Function to display tables
def display_data(selected_site):
    if selected_site:
        if file_paths[selected_site] and os.path.exists(file_paths[selected_site]):
            with st.spinner("Loading data..."):
                time.sleep(2)  # Simulate loading time
                data = load_data(file_paths[selected_site])

                # Filter data based on the search term
                if search_term:
                    st.subheader(f"Results for '{search_term}':")
                    mask = data.apply(lambda x: x.astype(str).str.contains(search_term, na=False, case=False)).any(
                        axis=1)
                    data = data[mask]

                # Display "New" entries first
                st.subheader("New Entries")
                if 'Status' in data.columns:
                    new_entries = data[data['Status'] == 'New']
                    if new_entries.empty:
                        st.write("No new entries.")
                    else:
                        st.dataframe(new_entries)

                    # After viewing, mark "New" entries as "Old"
                    data.loc[data['Status'] == 'New', 'Status'] = 'Old'

                # Filter and display data for the last 7 days next
                latest_data = filter_last_7_days(data)
                if not latest_data.empty:
                    st.subheader("Entries from the Last 7 Days")
                    st.dataframe(latest_data)  # Display the table
                else:
                    st.write("No entries in the last 7 days.")  # Add this to explicitly show if no data exists

                # Display all entries after the previous two
                st.subheader(f"All Data from {selected_site}")
                st.dataframe(data)

                # Save updated data with "Old" statuses
                if file_paths[selected_site].endswith('.csv'):
                    data.to_csv(file_paths[selected_site], index=False)
                else:
                    data.to_excel(file_paths[selected_site], index=False)

        else:
            st.warning("No data file available for this source.")

# Display the selected site's data by default
display_data(selected_site)

# Handle rescraping button action with hardcoded wait times and progress bar
if rescrape_button_sidebar:
    # Display a message in the sidebar
    sidebar_message_placeholder.markdown("### ⚠️ Scraping in progress! Please do not click anything. ⚠️")

    # Create a progress bar and update it based on the duration inside the placeholder
    progress_bar = progress_placeholder.progress(0)
    total_time = scrape_times.get(selected_rescrape, 150)

    # Start scraping and updating the progress bar concurrently
    if selected_rescrape == "All":
        functions = [scrape_isro_data, scrape_invest_india_data, scrape_nal_data, scrape_gem_data, scrape_dst_data,
                     scrape_bdl_data]
    else:
        functions = [globals()[f"scrape_{selected_rescrape.lower()}_data"]]

    total_steps = total_time  # Set total steps to the scrape time

    for i in range(total_steps):
        time.sleep(1)
        progress_bar.progress((i + 1) / total_steps)  # Update progress bar
        if i == 0:  # Start scraping only once the progress starts
            for function in functions:
                function()  # Perform scraping for the current function

    st.success(f"{selected_rescrape} scraping completed!")
    progress_placeholder.empty()  # Clear the progress bar after scraping

    # Remove the sidebar message after scraping completes
    sidebar_message_placeholder.empty()
