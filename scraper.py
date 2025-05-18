import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import pandas as pd
import time
import os
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import requests
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime


# Function to initialize Chrome driver with headless mode
def get_headless_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Enable headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)


# Function to load existing data and mark all previous "New" entries as "Old"
def load_existing_data(file_path, unique_col):
    if os.path.exists(file_path):
        existing_data = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)
        if 'Status' in existing_data.columns:
            existing_data['Status'] = 'Old'
        else:
            existing_data['Status'] = 'Old'
    else:
        existing_data = pd.DataFrame(columns=[unique_col, 'Status'])
    return existing_data


# Function to update status for new and existing entries using vectorized operations
def update_status(new_data, existing_data, unique_col):
    if not existing_data.empty:
        new_data['Status'] = np.where(new_data[unique_col].isin(existing_data[unique_col]), 'Old', 'New')
        final_data = pd.concat([existing_data, new_data], ignore_index=True).drop_duplicates(subset=[unique_col])
    else:
        new_data['Status'] = 'New'
        final_data = new_data
    return final_data


# ISRO Scraping (with Scraped Date)
def scrape_isro_data():
    driver = get_headless_driver()
    driver.get("https://eproc.isro.gov.in/home.html")
    data = []
    current_date = datetime.now().strftime('%Y-%m-%d')

    def scrape_data():
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "tr.even, tr.odd"))
            )
            rows = driver.find_elements(By.CSS_SELECTOR, "tr.even, tr.odd")
            for row in rows:
                try:
                    tender_id = row.find_element(By.CSS_SELECTOR, "td:nth-child(1)").text.strip()
                    organization = row.find_element(By.CSS_SELECTOR, "td:nth-child(2)").text.strip()
                    title = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)").text.strip()
                    submission_date = row.find_element(By.CSS_SELECTOR, "td:nth-child(4)").text.strip()
                    opening_date = row.find_element(By.CSS_SELECTOR, "td:nth-child(5)").text.strip()
                    tender_document = row.find_element(By.CSS_SELECTOR, "td:nth-child(6) a").get_attribute("href")
                    data.append(
                        [tender_id, organization, title, submission_date, opening_date, tender_document, current_date])
                except NoSuchElementException:
                    continue
        except TimeoutException:
            pass

    # Scrape the data
    for _ in range(3):
        scrape_data()
        try:
            next_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a[aria-controls='tenderListTable'][data-dt-idx='4']"))
            )
            if next_button.is_enabled():
                next_button.click()
                time.sleep(3)
            else:
                break
        except TimeoutException:
            break

    driver.quit()

    # Convert scraped data to DataFrame
    new_data = pd.DataFrame(data, columns=["Tender ID", "Organization", "Title", "Submission Date", "Opening Date",
                                           "Tender Document", "Scraped Date"])

    # Load existing data and update status
    existing_data = load_existing_data("isro_scraped_data.csv", "Tender ID")
    final_data = update_status(new_data, existing_data, "Tender ID")

    # Save the final data
    final_data.to_csv("isro_scraped_data.csv", index=False)
    print(f"ISRO data scraped and saved. Total rows: {len(final_data)}")


# GEM Scraping (with Scraped Date)
def scrape_gem_data():
    driver = get_headless_driver()
    driver.get("https://mkp.gem.gov.in/browse_nodes/browse_list#!/categories")

    wait = WebDriverWait(driver, 10)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody")))
    except TimeoutException:
        print("Failed to load GEM page or table.")
        driver.quit()
        return

    data = []
    page_count = 0
    max_pages = 35
    current_date = datetime.now().strftime('%Y-%m-%d')

    while page_count < max_pages:
        print(f"Scraping page {page_count + 1} of GEM data...")

        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            if not rows:
                print("No rows found on this page.")
                break

            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 3:
                    id_value = cells[0].text.strip()
                    descriptor = cells[1].text.strip()
                    specs = cells[2].text.strip()

                    # Append the scrape date
                    data.append([descriptor, id_value, specs, current_date])

        except Exception as e:
            print(f"Error scraping data on page {page_count + 1}: {e}")
            break

        try:
            next_button = driver.find_element(By.CSS_SELECTOR, "li.pagination-next.ng-scope > a")
            if next_button.is_enabled():
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(3)
                wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table tbody tr")))
                page_count += 1
            else:
                break
        except TimeoutException:
            break
        except Exception as e:
            break

    driver.quit()

    if data:
        new_data = pd.DataFrame(data, columns=['Descriptor', 'ID', 'Specs', 'Scraped Date'])

        if os.path.exists("scraped_gem_data.csv") and os.path.getsize("scraped_gem_data.csv") > 0:
            existing_data = pd.read_csv("scraped_gem_data.csv")
        else:
            existing_data = pd.DataFrame(columns=['Descriptor', 'ID', 'Specs', 'Scraped Date'])

        new_data['Status'] = np.where(new_data['ID'].isin(existing_data['ID']), 'Old', 'New')
        final_data = pd.concat([existing_data, new_data], ignore_index=True).drop_duplicates(subset=['ID'])

        final_data.to_csv("scraped_gem_data.csv", index=False, encoding='utf-8')
        final_data.to_excel("scraped_gem_data.xlsx", index=False)
        print(f"GEM data scraped and saved. Total rows: {len(final_data)}")
    else:
        print("No data scraped from GEM.")


# Invest India Scraping (with Scraped Date)
def scrape_invest_india_data():
    driver = get_headless_driver()
    driver.get('https://www.investindia.gov.in/request-for-proposal')

    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'h3')))
    h3_elements = driver.find_elements(By.TAG_NAME, 'h3')
    data = []
    current_date = datetime.now().strftime('%Y-%m-%d')

    for h3 in h3_elements:
        tender_head_text = h3.text.strip()
        if tender_head_text:
            data.append({
                'Tender Head': tender_head_text,
                'Tender List Inner': '',
                'Scraped Date': current_date
            })

    WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.tender-list-inner')))
    div_elements = driver.find_elements(By.CSS_SELECTOR, 'div.tender-list-inner')

    for i, div in enumerate(div_elements):
        tender_list_text = div.text.strip()
        if tender_list_text and i < len(data):
            data[i]['Tender List Inner'] = tender_list_text

    # Convert scraped data to DataFrame
    new_data = pd.DataFrame(data)

    # Load existing data and update status
    existing_data = load_existing_data("invest_india_data.xlsx", "Tender Head")
    final_data = update_status(new_data, existing_data, "Tender Head")

    # Save the final data
    final_data.to_excel('invest_india_data.xlsx', index=False)
    print("Invest India data extracted and saved to invest_india_data.xlsx")
    driver.quit()


# NAL Scraping (with Scraped Date)
def scrape_nal_data():
    url = 'https://www.nal.res.in/en/tender-purchase'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    rows = table.find_all('tr')
    scraped_data = []
    current_date = datetime.now().strftime('%Y-%m-%d')

    for row in rows[1:]:
        cells = row.find_all('td')
        cell_data = [cell.text.strip() for cell in cells]
        cell_data.append(current_date)  # Add scraped date
        scraped_data.append(cell_data)

    df = pd.DataFrame(scraped_data)

    if len(rows) > 1:
        header_row = [cell.text.strip() for cell in rows[0].find_all('td')]
        header_row.append("Scraped Date")  # Append the "Scraped Date" column
        df.columns = header_row

    # Load existing data and update status
    existing_data = load_existing_data("nal_tender_data.xlsx", df.columns[0])
    new_entries = df[~df.iloc[:, 0].isin(existing_data.iloc[:, 0])].copy()
    new_entries['Status'] = 'New'
    existing_data['Status'] = 'Old'

    final_df = pd.concat([existing_data, new_entries], ignore_index=True) if not new_entries.empty else existing_data
    final_df.to_excel('nal_tender_data.xlsx', index=False)
    print("NAL data scraped and saved to nal_tender_data.xlsx")


# DST Scraping (with Scraped Date)
def scrape_dst_data():
    driver = get_headless_driver()
    driver.get("https://dst.gov.in/search/node/grants")

    data = []
    current_date = datetime.now().strftime('%Y-%m-%d')
    while True:
        results = driver.find_elements(By.CSS_SELECTOR, ".search-result")
        for result in results:
            title = result.find_element(By.CSS_SELECTOR, "h3 a").text
            link = result.find_element(By.CSS_SELECTOR, "h3 a").get_attribute("href")
            description = result.find_element(By.CSS_SELECTOR, ".search-snippet").text
            data.append([title, link, description, current_date])

        try:
            next_button = driver.find_element(By.CSS_SELECTOR, ".pager-next a")
            next_button.click()
            time.sleep(3)
        except:
            break

    driver.quit()

    df = pd.DataFrame(data, columns=["Title", "Link", "Description", "Scraped Date"])

    # Load existing data and update status
    existing_data = load_existing_data("scraped_DST.csv", "Title")
    final_data = update_status(df, existing_data, "Title")

    # Save the final data
    final_data.to_csv("scraped_DST.csv", index=False)
    print(f"DST data scraped and saved to scraped_DST.csv")


# Srijan Scraping (with Scraped Date)
def scrape_srijan_data():
    url = 'https://srijandefence.gov.in/'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    cards = soup.find_all('div', class_='card-body homecard')
    titles, dates = [], []
    current_date = datetime.now().strftime('%Y-%m-%d')
    for card in cards:
        title_tag = card.find('p', class_='product-title')
        date_tag = card.find('div', id=lambda x: x and x.startswith('dlproduct_Tr233_'))
        title = title_tag.get_text(strip=True) if title_tag else "No title"
        date = date_tag.get_text(strip=True).replace('Last Updated :-', '').strip() if date_tag else "No date"
        titles.append(title)
        dates.append(current_date)

    new_data = pd.DataFrame({'Product Title': titles, 'Scraped Date': dates})

    # Load existing data and update status
    existing_data = load_existing_data("srijan_defence_products_with_dates.xlsx", "Product Title")
    final_data = update_status(new_data, existing_data, "Product Title")

    # Save the final data
    final_data.to_excel('srijan_defence_products_with_dates.xlsx', index=False)
    print("Srijan data scraped and saved to srijan_defence_products_with_dates.xlsx")


# BDL Scraping (with Scraped Date)
def scrape_bdl_data():
    url = 'https://bdltenders.abcprocure.com/ssoeprocurement/EProc.jsp'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    tenders = []
    current_date = datetime.now().strftime('%Y-%m-%d')

    tender_divs = soup.find_all('div', class_='tender m-top_1')

    for tender in tender_divs:
        try:
            tender_fields = tender.find('td', class_='a-left tenderfields').text.strip()
            department_unit = tender_fields.split('|')[0].split(': ')[1].strip()
            tender_no = tender_fields.split('|')[1].split(': ')[1].strip()
            due_date = tender_fields.split('|')[2].split(': ')[1].strip()

            tender_link = tender.find('a')['href']
            tender_description = tender.find('a').text.strip()

            tender_id_corrigendum = tender.find('td', class_='a-right').text.strip()
            tender_id = tender_id_corrigendum.split('|')[0].split(': ')[1].strip()
            corrigendum = tender_id_corrigendum.split('|')[1].split(': ')[1].strip()

            tenders.append({
                'Department/Unit': department_unit,
                'Tender No': tender_no,
                'Due Date': due_date,
                'Tender Description': tender_description,
                'Tender Link': f"https://bdltenders.abcprocure.com/ssoeprocurement/{tender_link}",
                'Tender ID': tender_id,
                'Corrigendum': corrigendum,
                'Scraped Date': current_date  # Adding the scrape date
            })

        except Exception as e:
            print(f"Error extracting tender info: {e}")
            continue

    # Convert the data to a DataFrame
    new_data = pd.DataFrame(tenders)

    # Load existing data and update status
    existing_data = load_existing_data("bdl_tenders.csv", "Tender ID")

    # Only keep new rows that do not already exist in the file (based on "Tender ID")
    final_data = update_status(new_data, existing_data, "Tender ID")
    # Sort the data by 'Scraped Date' in descending order
    final_data = final_data.sort_values(by="Scraped Date", ascending=False)

    # Save the final data, dropping any duplicates (based on the "Tender ID")
    final_data.drop_duplicates(subset="Tender ID", keep="first", inplace=True)
    final_data.to_csv("bdl_tenders.csv", index=False)
    print(f"BDL data scraped and saved. Total rows: {len(final_data)}")

# Function to run each scraper
def run_scraper(scraper_func):
    scraper_func()


# Main function to execute all scraping functions in parallel
def main():
    scrapers = [
        scrape_isro_data,
        scrape_invest_india_data,
        scrape_nal_data,
        scrape_gem_data,
        scrape_dst_data,
        scrape_bdl_data
    ]

    for scraper in scrapers:
        print(f"Running {scraper.__name__}...")
        scraper()
        time.sleep(2)  # Optional delay between scrapers


if __name__ == "__main__":
    main()

