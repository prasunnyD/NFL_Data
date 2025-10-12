import nfl_data_py as nfl
import polars as pl
import players
import duckdb
import pandas as pd
from scraping import PlaywrightScraper
from teams import sharp_defense_stats, sumer_advanced_stats


if __name__ == "__main__":
    # Test with multiple URLs
    # urls = [
    #     "https://sumersports.com/teams/defensive/",
    #     "https://sumersports.com/teams/offensive/",]
    
    # print("Testing PlaywrightScraper with multiple URLs...")
    
    # with PlaywrightScraper(headless=True, min_delay=1.0, max_delay=2.0) as scraper:
    #     results = scraper.scrape_multiple_urls(urls, wait_time=5)
        
    #     # Process results
    #     for result in results:
    #         if result.success:
    #             df = pl.from_pandas(result.dataframe)
    #             print(f"\n✅ Successfully scraped: {result.url}")
    #             print(f"   Shape: {df.shape}")
    #             print(f"   Tables found: {result.tables_found}")
    #             print(df.head(5))
    #         else:
    #             print(f"\n❌ Failed to scrape: {result.url}")
    #             print(f"   Error: {result.error}")
        
    #     # Summary
    #     successful = sum(1 for r in results if r.success)
    #     print(f"\n📊 Summary: {successful}/{len(results)} URLs scraped successfully")

    defense_df, offense_df = sumer_advanced_stats()
    print("Final DataFrame:")
    print(defense_df.columns)
    print(defense_df.head(5))
    print(offense_df.columns)
    print(offense_df.head(5))

