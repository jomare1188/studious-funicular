# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas",
#     "regex",
#     "rich",
#     "selenium",
#     "webdriver-manager",
# ]
# ///
import logging
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import regex as re
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from random import randint
import pandas as pd
from datetime import datetime
from rich.logging import RichHandler
import argparse

# Set up logging
FORMAT = "%(message)s"

logging.basicConfig(
    format=FORMAT,
    level="INFO",
    handlers=[RichHandler(show_time=False, show_path=False, markup=False)],
)

log_text = logging.getLogger("rich")
log_text.setLevel(20)

URL = r"https://scholar.google.com/scholar?q="


def start_driver() -> webdriver.Firefox:
    '''
    Start a Selenium WebDriver session for Google Scholar
    
    Returns:
        WebDriver: The initialized Selenium WebDriver instance.
    '''
    driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
    log_text.info("Driver ok!")
    driver.implicitly_wait(5)
    return driver


def extract_article_data(driver, bioproject_id: str|None = None, filter_by_bioproject: bool = False) -> list[dict]:
    """
    Extract article names, citation counts, and links from Google Scholar results
    Limited to first 5 results, with optional filtering by bioproject_id in article text

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
        bioproject_id (str|None): The ID of the bioproject to filter results by (if any).
        filter_by_bioproject (bool): Whether to filter results by bioproject_id.

    Returns:
        list: A list of dictionaries containing article data (title, link, citations).
    """
    articles = []
    
    try:
        # Wait for results to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-lid]'))
        )
        
        # Find all article containers
        article_elements = driver.find_elements(By.CSS_SELECTOR, '[data-lid]')
        
        # Limit to first 5 results
        article_elements = article_elements[:5]
        
        for element in article_elements:
            article_data = {}
            
            try:
                # Extract article title and link
                title_element = element.find_element(By.CSS_SELECTOR, 'h3 a')
                article_data['title'] = title_element.text.strip()
                article_data['link'] = title_element.get_attribute('href')
                
                # Extract citation count
                citation_count = 0
                try:
                    citation_element = element.find_element(By.CSS_SELECTOR, 'a[href*="cites"]')
                    citation_text = citation_element.text
                    # Extract number from "Cited by X" text
                    citation_match = re.search(r'Cited by (\d+)', citation_text)
                    if citation_match:
                        citation_count = int(citation_match.group(1))
                except NoSuchElementException:
                    # No citations found for this article
                    pass
                
                article_data['citations'] = citation_count
                
                # If filtering is enabled, check if bioproject_id is in the article description
                if filter_by_bioproject and bioproject_id:
                    try:
                        # Look for the description/snippet text below the title
                        description_element = element.find_element(By.CSS_SELECTOR, '.gs_rs')
                        description_text = description_element.text.strip()
                        article_data['description'] = description_text
                        
                        # Only include article if bioproject_id is found in description
                        if bioproject_id.upper() not in description_text.upper():
                            continue
                            
                    except NoSuchElementException:
                        # No description found, skip if filtering is enabled
                        continue
                
                # Add bioproject_id to article data for tracking
                article_data['bioproject_id'] = bioproject_id
                
                # Only add if we found a title
                if article_data['title']:
                    articles.append(article_data)
                    
            except NoSuchElementException:
                # Skip this element if title not found
                continue
                
    except TimeoutException:
        log_text.error("Timeout waiting for search results to load")
        return []
    
    return articles

def scrape_multiple_bioprojects(bioproject_ids: list[str], filter_by_bioproject: bool = False, 
                               restart_interval: int|None = None, df: pd.DataFrame|None = None,
                               bioproject_col: str = 'BioProject', main_csv_file: str|None = None) -> dict[str, list[dict]]:
    """
    Scrape Google Scholar articles for multiple bioproject IDs using a single driver session
    with optional periodic driver restarts and incremental DataFrame updates

    Args:
        bioproject_ids (list[str]): List of bioproject IDs to search for.
        filter_by_bioproject (bool): Whether to filter results by bioproject_id.
        restart_interval (int|None): Number of bioprojects to process before restarting driver.
                                   If None, no restart occurs.
        df (pd.DataFrame|None): Original DataFrame to update with results.
        bioproject_col (str): Name of the bioproject column in DataFrame.
        main_csv_file (str|None): Path to main CSV file to save after each bioproject.

    Returns:
        dict: Dictionary mapping bioproject_id to list of article data.
    """
    all_results = {}
    driver = None
    processed_count = 0
    
    # Set random restart interval if not specified
    if restart_interval is None:
        restart_interval = randint(3, 13)
        log_text.info(f"Driver restart interval: {restart_interval}")

    try:
        for i, bioproject_id in enumerate(bioproject_ids):
            log_text.info(f"Processing bioproject {i+1}/{len(bioproject_ids)}: {bioproject_id}")
            
            # Start driver if needed (first run or after restart)
            if driver is None:
                driver = start_driver()
                processed_count = 0
            
            # Navigate to search URL for this bioproject
            scholar_url = URL + bioproject_id + '&hl=en'
            driver.get(scholar_url)
            
            # Add respectful delay
            time.sleep(randint(2, 5))
            
            # Extract articles for this bioproject
            articles = extract_article_data(driver, bioproject_id, filter_by_bioproject)
            all_results[bioproject_id] = articles
            
            if filter_by_bioproject:
                log_text.info(f"Found {len(articles)} articles containing '{bioproject_id}' in description")
            else:
                log_text.info(f"Found {len(articles)} articles for {bioproject_id}")

            # Update DataFrame and save main CSV after each bioproject if provided
            if df is not None and main_csv_file is not None:
                update_dataframe_single_bioproject(df, bioproject_id, articles, bioproject_col)
                df.to_csv(main_csv_file, index=False, encoding='utf-8')
                log_text.info(f"Updated main CSV: {main_csv_file}")
            
            processed_count += 1
            
            # Check if we need to restart the driver
            if processed_count >= restart_interval and i < len(bioproject_ids) - 1:  # Don't restart on last iteration
                log_text.info("Restarting driver to avoid detection...")
                driver.quit()
                driver = None
                # Random delay before restarting
                time.sleep(randint(3, 8))
                restart_interval = randint(10, 20)
                log_text.info(f"New restart interval: {restart_interval}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if driver:
            driver.quit()
    
    return all_results

def save_individual_bioproject_results(bioproject_id: str, articles: list[dict], output_dir: str = "scholar_results"):
    """
    Save results for a single bioproject to individual files (JSON and CSV)
    
    Args:
        bioproject_id (str): The bioproject ID
        articles (list[dict]): List of article data for this bioproject
        output_dir (str): Directory to save files
    """
    import os
    import json
    import pandas as pd
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save as JSON
    json_file = os.path.join(output_dir, f"{bioproject_id}_articles.json")
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump({
            'bioproject_id': bioproject_id,
            'articles': articles,
            'total_articles': len(articles),
            'scrape_timestamp': datetime.now().isoformat(),
        }, f, indent=2, ensure_ascii=False)
    
    # Save as CSV for easy analysis
    if articles:
        df_articles = pd.DataFrame(articles)
        csv_file = os.path.join(output_dir, f"{bioproject_id}_articles.csv")
        df_articles.to_csv(csv_file, index=False, encoding='utf-8')
        log_text.info(f"Saved {len(articles)} articles for {bioproject_id} to {json_file} and {csv_file}")
    else:
        log_text.warning(f"No articles found for {bioproject_id}")

def update_dataframe_single_bioproject(df: pd.DataFrame, bioproject_id: str, articles: list[dict], 
                                      bioproject_col: str = 'BioProject') -> None:
    """
    Update DataFrame with results for a single bioproject (in-place modification)
    
    Args:
        df (pd.DataFrame): DataFrame to update (modified in-place)
        bioproject_id (str): The bioproject ID that was processed
        articles (list[dict]): List of article data for this bioproject
        bioproject_col (str): Name of the bioproject column in df
    """
    # Initialize columns if they don't exist
    if 'scholar_articles_count' not in df.columns:
        df['scholar_articles_count'] = 0
    if 'scholar_total_citations' not in df.columns:
        df['scholar_total_citations'] = 0
    if 'scholar_avg_citations' not in df.columns:
        df['scholar_avg_citations'] = 0.0
    if 'scholar_max_citations' not in df.columns:
        df['scholar_max_citations'] = 0
    if 'scholar_top_article' not in df.columns:
        df['scholar_top_article'] = None
    if 'scholar_processed' not in df.columns:
        df['scholar_processed'] = False
    if 'scholar_timestamp' not in df.columns:
        df['scholar_timestamp'] = None
    if 'scholar_links' not in df.columns:
        df['scholar_links'] = None

    # Calculate statistics for this bioproject
    total_articles = len(articles)
    total_citations = sum(article['citations'] for article in articles)
    avg_citations = total_citations / total_articles if total_articles > 0 else 0
    max_citations = max((article['citations'] for article in articles), default=0)
    top_article = max(articles, key=lambda x: x['citations'])['title'] if articles else None
    links = [article['link'] for article in articles] if articles else []
    
    # Update all rows with this bioproject_id
    log_text.info(f"Updating DataFrame for {bioproject_id}: {total_articles} articles, {total_citations} total citations")
    mask = df[bioproject_col] == bioproject_id
    df.loc[mask, 'scholar_articles_count'] = total_articles
    df.loc[mask, 'scholar_total_citations'] = total_citations
    df.loc[mask, 'scholar_avg_citations'] = avg_citations
    df.loc[mask, 'scholar_max_citations'] = max_citations
    df.loc[mask, 'scholar_top_article'] = top_article
    df.loc[mask, 'scholar_processed'] = True
    df.loc[mask, 'scholar_timestamp'] = datetime.now().isoformat()
    df.loc[mask, 'scholar_links'] = pd.Series([list(links) for _ in range(mask.sum())], index=df.index[mask])


def update_dataframe_with_results(df: pd.DataFrame, results: dict[str, list[dict]], 
                                bioproject_col: str = 'BioProject') -> pd.DataFrame:
    """
    Add article data to the original dataframe
    
    Args:
        df (pd.DataFrame): Original dataframe with bioproject column
        results (dict): Results from scraping (bioproject_id -> articles)
        bioproject_col (str): Name of the bioproject column in df
    
    Returns:
        pd.DataFrame: Updated dataframe with new columns
    """
    # Create summary statistics for each bioproject
    bioproject_stats = {}
    for bioproject_id, articles in results.items():
        bioproject_stats[bioproject_id] = {
            'total_articles_found': len(articles),
            'total_citations': sum(article['citations'] for article in articles),
            'avg_citations': sum(article['citations'] for article in articles) / len(articles) if articles else 0,
            'max_citations': max((article['citations'] for article in articles), default=0),
            'top_article_title': max(articles, key=lambda x: x['citations'])['title'] if articles else None
        }
    
    # Add new columns to dataframe
    df_copy = df.copy()
    df_copy['scholar_articles_count'] = df_copy[bioproject_col].map(
        lambda x: bioproject_stats.get(x, {}).get('total_articles_found', 0)
    )
    df_copy['scholar_total_citations'] = df_copy[bioproject_col].map(
        lambda x: bioproject_stats.get(x, {}).get('total_citations', 0)
    )
    df_copy['scholar_avg_citations'] = df_copy[bioproject_col].map(
        lambda x: bioproject_stats.get(x, {}).get('avg_citations', 0)
    )
    df_copy['scholar_max_citations'] = df_copy[bioproject_col].map(
        lambda x: bioproject_stats.get(x, {}).get('max_citations', 0)
    )
    df_copy['scholar_top_article'] = df_copy[bioproject_col].map(
        lambda x: bioproject_stats.get(x, {}).get('top_article_title', None)
    )
    
    return df_copy

def scrape_scholar_articles_batch(bioproject_ids: list[str], filter_by_bioproject: bool = False,
                                 output_file: str|None = None, save_individual: bool = True,
                                 output_dir: str = "scholar_results", df: pd.DataFrame|None = None,
                                 bioproject_col: str = 'BioProject', 
                                 main_csv_file: str|None = None) -> dict[str, list[dict]]:
    """
    Wrapper function to scrape multiple bioproject IDs and save results

    Args:
        bioproject_ids (list[str]): List of bioproject IDs to search for.
        filter_by_bioproject (bool): Whether to filter results by bioproject_id.
        output_file (str|None): Optional file path to save combined results as JSON.
        save_individual (bool): Whether to save individual files for each bioproject.
        output_dir (str): Directory to save individual files.
        df (pd.DataFrame|None): Original DataFrame to update incrementally.
        bioproject_col (str): Name of the bioproject column in DataFrame.
        main_csv_file (str|None): Path to main CSV file to save after each bioproject.

    Returns:
        dict: Dictionary mapping bioproject_id to list of article data.
    """
    results = scrape_multiple_bioprojects(bioproject_ids, filter_by_bioproject, 
                                        df=df, bioproject_col=bioproject_col, 
                                        main_csv_file=main_csv_file)
    
    # Save individual files for each bioproject
    if save_individual:
        for bioproject_id, articles in results.items():
            save_individual_bioproject_results(bioproject_id, articles, output_dir)
    
    # Save combined results if output file specified
    if output_file:
        import json
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'scrape_timestamp': datetime.now().isoformat(),
                'total_bioprojects': len(bioproject_ids),
                'results': results
            }, f, indent=2, ensure_ascii=False)
        log_text.info(f"Combined results saved to {output_file}")

    # Print summary
    total_articles = sum(len(articles) for articles in results.values())
    log_text.info(f"\nSummary: Processed {len(bioproject_ids)} bioprojects, found {total_articles} total articles")

    return results

def read_df(file_path: str) -> pd.DataFrame:
    """
    Read the DataFrame from a CSV file.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
    return pd.read_csv(file_path, header=0, low_memory=False)

def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Searches PMID Database for SRA Runs",
        add_help=True,
    )
    parser.add_argument(
        "--input", type=str, required=True, help="Path to the input CSV file"
    )
    parser.add_argument(
        "--output", type=str, required=True, help="Path to the output CSV file"
    )
    return parser

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    df = read_df(args.input)
    # Filter rows where PMID_count == 0
    if 'scholar_processed' in df.columns: # first execution
        filtered_df = df[(df['PMID_count'] == 0) & df['scholar_processed']]
    else:
        filtered_df = df[df['PMID_count'] == 0]

    # Get unique BioProject IDs from that filtered set
    bioproject_ids = filtered_df['BioProject'].unique().tolist()
    log_text.info(f"Found {len(bioproject_ids)} unique bioprojects to process")

    # Create backup of original file
    backup_file = args.input.replace('.csv', '_backup.csv')
    df.to_csv(backup_file, index=False)
    log_text.info(f"Backup saved to {backup_file}")
    
    # Scrape articles with incremental CSV updates
    main_csv_file = args.output
    output_file = args.output.replace('.csv', '') + "_combined_scholar_results.json"
    results = scrape_scholar_articles_batch(
        bioproject_ids=bioproject_ids, 
        filter_by_bioproject=False,
        output_file=output_file,  # Combined results
        save_individual=True,  # Individual files per bioproject
        output_dir="scholar_results",  # Directory for individual files
        df=df,  # Your DataFrame
        bioproject_col='BioProject',  # Column name
        main_csv_file=main_csv_file  # Main CSV file to update after each bioproject
    )

    log_text.info(f"Final updated DataFrame saved to {main_csv_file}")
