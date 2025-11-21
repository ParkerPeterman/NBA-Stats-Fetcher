import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import os
import time

# Import the necessary library for fetching NBA data
# You must install this: pip install nba-api
from nba_api.stats.endpoints import leaguedashplayerstats

# --- Configuration & Path Setup ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
DATA_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, 'nba_data.sqlite')
TABLE_NAME = 'all_player_season_totals'

# --- Multi-Season Configuration ---
# NBA data is reliably available from 1996-97 onwards for this endpoint.
START_YEAR = 1996 
# We'll scrape up to the 2023-24 season (the start year of the last season)
END_YEAR = 2023 

def get_season_list(start_year: int, end_year: int) -> list[str]:
    """
    Generates a list of season strings in the 'YYYY-YY' format.
    Example: 1996, 2023 -> ['1996-97', '1997-98', ..., '2023-24']
    """
    seasons = []
    for year in range(start_year, end_year + 1):
        # The second part is the last two digits of the end year (year + 1)
        season_end_two_digits = str(year + 1)[-2:]
        season_str = f"{year}-{season_end_two_digits}"
        seasons.append(season_str)
    return seasons

def fetch_nba_stats(season: str) -> pd.DataFrame:
    """
    Fetches comprehensive regular season statistics for all players in a given season 
    using the official NBA API.
    """
    print(f"-> Fetching player stats for the {season} Regular Season...")
    
    try:
        player_stats = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season,
            timeout=30 
        )

        df = player_stats.get_data_frames()[0]
        
        # --- FIX APPLIED HERE ---
        # Inject the human-readable season string into a new column called 'Season'
        df['Season'] = season 
        
        # Display summary info
        print(f"-> Success: Downloaded {len(df)} player records for {season}.")
        return df

    except Exception as e:
        print(f"ERROR during data fetching for {season}. Skipping this season.")
        print(f"Error Details: {e}")
        return pd.DataFrame()

def save_to_sqlite(df: pd.DataFrame, engine):
    """
    Writes the DataFrame to the specified SQLite database path using 'append'.
    """
    if df.empty:
        return

    try:
        # NOTE: We use 'append' here so each season's data is added to the same table.
        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists='append', 
            index=False           
        )

    except Exception as e:
        # Use the SEASON column we injected for better logging
        season_id = df['Season'].iloc[0] if not df.empty and 'Season' in df.columns else 'Unknown Season'
        print(f"FATAL ERROR during data write operation for {season_id}.")
        print(f"Error Details: {e}")

def run_nba_pipeline():
    """
    Main function to execute the full data pipeline, iterating over multiple seasons.
    """
    season_list = get_season_list(START_YEAR, END_YEAR)
    print(f"\n--- NBA Data Pipeline Starting ({len(season_list)} Seasons: {season_list[0]} to {season_list[-1]}) ---")
    
    # 1. Establish the Connection Engine
    try:
        engine = create_engine(f'sqlite:///{DB_FILE}')
        print(f"Database file located at: {DB_FILE}")
    except Exception as e:
        print(f"FATAL ERROR: Could not connect or create database engine.")
        print(f"Error Details: {e}")
        return

    # 2. Iterate and process each season
    total_records_saved = 0
    
    # Before starting the loop, delete the old table to ensure a clean start
    print(f"\n-> Clearing existing data in '{TABLE_NAME}' before starting the multi-season scrape.")
    with engine.connect() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
        connection.commit()
    
    print("\n--- Starting Season Downloads (This may take several minutes) ---")
    for season in season_list:
        nba_stats_df = fetch_nba_stats(season)
        if not nba_stats_df.empty:
            save_to_sqlite(nba_stats_df, engine)
            total_records_saved += len(nba_stats_df)
            print(f"-> Saved {len(nba_stats_df)} records for {season}. Total saved: {total_records_saved}")
            
        # Pause for 1 second to respect the NBA API's rate limits
        time.sleep(1) 
    
    print(f"\n--- Data Scrape Complete ---")
    print(f"Total records saved across all seasons: {total_records_saved}")
    
    # 3. Read and Verify a sample of the data using a multi-season query
    try:
        # UPDATED: We now select the 'Season' column instead of the non-existent 'SEASON_ID'
        verification_query = text(f"""
            SELECT PLAYER_NAME, Season, PTS, REB, AST, STL, BLK 
            FROM {TABLE_NAME} 
            WHERE PLAYER_NAME IN ('LeBron James', 'Michael Jordan', 'Luka Doncic')
            ORDER BY PLAYER_NAME, Season
        """)
        player_comparison_df = pd.read_sql(verification_query, con=engine)
        
        print("\n--- Verification: Sample Player Season Totals (including Rebounds, Steals, Blocks) ---")
        print(player_comparison_df.to_markdown(index=False))

    except Exception as e:
        print(f"\nVerification failed: Could not read data back from the multi-season table. Check if the script finished running.")
        print(f"Error Details: {e}")

    # Final check print statement to confirm the file location
    print(f"\nSUCCESS: The full database file, '{os.path.basename(DB_FILE)}', is saved here. Please look for the 'Season' column.")
    print(f"FULL PATH: {os.path.abspath(DB_FILE)}")


# Run the pipeline
if __name__ == '__main__':
    run_nba_pipeline()
