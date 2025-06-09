import pandas as pd
import asyncio
from aiohttp import ClientSession
from aiohttp.http_exceptions import HttpBadRequest
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm
import os
import time
from dotenv import set_key, load_dotenv, find_dotenv

from place_geocoding import api_call

DOTENV_PATH = find_dotenv()
load_dotenv()
SAVE_FILE_PATH = "./Data/census.csv"
API_RATE_LIMIT = 20 # Per second

census_vars = pd.read_csv("census_variables.csv")
CSV_header_str = "City,State,PlaceCode,StateCode," + ",".join(census_vars["Attribute"])

def clear_census_csv() -> None:
    with open(SAVE_FILE_PATH, "w") as file:
        file.write(f"{CSV_header_str}\n")

def save_finished_idx(idx: int) -> None:
    set_key(DOTENV_PATH, "CENSUS_FINISHED_IDX", str(idx), quote_mode="never")
def load_finished_idx() -> int:
    try:
        return int(os.getenv("CENSUS_FINISHED_IDX", default="None"))
    except ValueError:
        raise ValueError("Census Finished Index must be an int")

async def fetch_census(df: pd.DataFrame, start_from_beginning: bool) -> None:
    limiter = AsyncLimiter(max_rate=20, time_period=1)
    places_done = set()
    places_lock = asyncio.Lock()
    places_not_saved = {}

    # Confirm overwrite if previously finished
    if start_from_beginning:     
        clear_census_csv()
        save_finished_idx(0)
        os.environ["CENSUS_FINISHED_IDX"] = '0'
    if os.getenv("CENSUS_FINISHED_ALL") != '0':
        if input(f"Previously finished all. Continue starting at row {os.getenv("CENSUS_FINISHED_IDX")} (y/[n])? ").lower() == 'y':
            set_key(DOTENV_PATH, "CENSUS_FINISHED_ALL", "0", quote_mode="never")
        else:
            return

    if start_from_beginning:     
        start_idx = 0
        print("Starting from beginning/row 0:")
    else:
        if os.path.isfile(SAVE_FILE_PATH):
            # Load previously saved places
            places_done = set()#{(row.City, row.State) for row in pd.read_csv(SAVE_FILE_PATH).itertuples()}

        start_idx = load_finished_idx()+1
        print(f"Starting from row {start_idx}")
    
    # Helper wrapper function to avoid duplicate calls, know city/state(from input df) as calls return
    async def api_call_wrapper(session: ClientSession, limiter: AsyncLimiter, url: str, params: dict, city:str , state: str):
        if (city, state) in places_done:
            return (None, None, None)
        return (await api_call(session, limiter, url, params), city, state)
    
    df = df.iloc[start_idx:]
    
    async with ClientSession() as session:
        api_url = "https://api.census.gov/data/2020/dec/dp"
        params = {
            "key": os.getenv("CENSUS_API_KEY"),
            "get": ",".join(census_vars["Attribute"])
            # for: "place:XXXXX"
            # in: "state:XX"
        }
        tasks = (api_call_wrapper(session, limiter, api_url, {**params, "for": f"place:{row.PlaceCode}", 
                                                              "in": f"state:{row.StateCode}"}, row.City, row.State) # type: ignore
                for row in df.itertuples())

        num_done = start_idx
        for task in tqdm.as_completed(tasks, total=df.shape[0]):
            (status_code, api_data), city, state = await task

            if status_code is None:
                # City, State already successfully looked up
                num_done += 1
                pass 
            elif status_code != 200:
                print(f"\nStatus Code {status_code} for {city}, {state}, continuing")
                num_done += 1
            else:
                # List of 2 lists, second of which has correct data
                # Data list last 2 values are state, place, not useful
                num_done += 1
                # First returned list is var names, extract var values
                api_data = api_data[1]

                async with places_lock:
                    #if (city, state) not in places_done:
                    # Duplicate handling should be done by places_clean.csv creator
                    places_done.add((city, state))
                    places_not_saved[(city, state)] = api_data

            if (num_done % 200) == 0: # Save to file occasionally so can recover
                async with places_lock:
                    with open(SAVE_FILE_PATH, 'a') as file:
                        for (city, state), api_data in places_not_saved.items():
                            # Separate codes and data
                            state_code, place_code = api_data[-2:]
                            api_data = api_data[:-2]
                            file.write(f"{city},{state},{place_code},{state_code},{','.join(api_data)}\n")
                    places_not_saved = {}
                    save_finished_idx(num_done)
    
    # Save rest
    with open(SAVE_FILE_PATH, 'a') as file:
        for (city, state), api_data in places_not_saved.items():
            # Separate codes and data
            state_code, place_code = api_data[-2:]
            api_data = api_data[:-2]
            file.write(f"{city},{state},{place_code},{state_code},{','.join(api_data)}\n")
        places_not_saved = {}
        save_finished_idx(num_done)
    print("Done fetching census data for all places")

async def main():
    df = pd.read_csv("./Data/places_super_clean.csv", dtype=str)
    # City,State,StateCode,PlaceCode

    start_from_beginning = input("Start from beginning of Places (y/[n])? ").lower() == 'y'

    finished = False
    while not finished:
        try:
            await fetch_census(df, start_from_beginning)
            set_key(DOTENV_PATH, "CENSUS_FINISHED_ALL", "1", quote_mode="never")
            finished = True
        except HttpBadRequest as e:
                print(f"HTTP Request Error:\n{e}\nRetrying in 30s:")
                start_from_beginning = False
                time.sleep(15)
        except Exception as e:
            print(f"{e}\nRetrying in 30s:")
            start_from_beginning = False
            time.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())
