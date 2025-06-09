import pandas as pd
import asyncio
from aiohttp import ClientSession
from aiohttp.http_exceptions import HttpBadRequest
import aiohttp
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm
import os
import re
from dotenv import set_key, load_dotenv, find_dotenv
DOTENV_PATH = find_dotenv()
load_dotenv()
SAVE_FILE_PATH = "./Data/places.csv"
API_RATE_LIMIT = 20 # Per second

async def api_call(session: ClientSession, limiter: AsyncLimiter, url: str, params=None) -> tuple[int, dict]:
    # Error handling done by caller
    async with limiter:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            return (resp.status, await resp.json())

regex_rules = [
    (re.compile(r'\bst\.?\b'), "saint"),      # st. to saint
    (re.compile(r'\bmt\.?\b'), "mount"),      # mt. to mount
    (re.compile(r'[^a-z ]'), '')             # remove all except letters, space
]
def clean_city_name(city: str) -> str:
    city = city.strip().lower()
    for pattern, replacement in regex_rules:
        city = pattern.sub(replacement, city)
    return city.replace(" ","")

if __name__ == "__main__":
    def clear_place_csv() -> None:
        with open(SAVE_FILE_PATH, "w") as file:
            file.write("City,State,StateCode,PlaceCode\n")

    def save_finished_idx(idx: int) -> None:
        set_key(DOTENV_PATH, "GEOCODE_FINISHED_IDX", str(idx), quote_mode="never")
    def load_finished_idx() -> int:
        try:
            return int(os.getenv("GEOCODE_FINISHED_IDX", default="None"))
        except ValueError:
            raise ValueError("Geocode Finished Index must be an int")

    async def geocode(df: pd.DataFrame, start_from_beginning: bool) -> None:
        limiter = AsyncLimiter(max_rate=20, time_period=1)
        places_done = set()
        places_lock = asyncio.Lock()
        places_not_saved = {}

        # Confirm overwrite if previously finished
        if os.getenv("GEOCODE_FINISHED_ALL") != '0':
            if input("Previously finished all. Overwrite (y/[n])?").lower() == 'y':
                set_key(DOTENV_PATH, "GEOCODE_FINISHED_ALL", "0", quote_mode="never")
            else:
                return

        if start_from_beginning:     
            clear_place_csv()
            
            save_finished_idx(0)
            start_idx = 0
            print("Starting from beginning/row 0:")
        else:
            if os.path.isfile(SAVE_FILE_PATH):
                # Load previously saved places
                places_done = {(city, state) for _, (city, state, _, _) in pd.read_csv(SAVE_FILE_PATH).iterrows()}

            start_idx = load_finished_idx()+1
            print(f"Starting from row {start_idx}")
        
        # Helper wrapper function to avoid duplicate calls, know city/state(from input df) as calls return
        async def api_call_wrapper(session: ClientSession, limiter: AsyncLimiter, url: str, params: dict, city:str , state: str):
            if (city, state) in places_done:
                return (None, None, None)
            else:
                return (await api_call(session, limiter, url, params), city, state)
        
        df = df.iloc[start_idx:]
        
        async with ClientSession() as session:
            api_url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
            params = {
                "key": os.getenv("CENSUS_API_KEY"),
                #"x": long,
                #"y": lat,
                "layers": "28,29,30,31",
                "benchmark": "Public_AR_Current",
                "vintage": "Current_Current",
                "format": "json"
            }
            tasks = (api_call_wrapper(session, limiter, api_url, {**params, "x": long, "y": lat}, clean_city_name(city), state) 
                    for _, city, state, lat, long in df.itertuples())

            num_done = start_idx
            for task in tqdm.as_completed(tasks, total=df.shape[0]):
                (status_code, api_data), city, state = await task
                num_done += 1

                if api_data is None:
                    # City/State already successfully looked up
                    pass 
                elif status_code != 200:
                    print(f"Status Code {status_code} for {city}, {state}, continuing")
                    num_done += 1
                else:
                    data = api_data["result"]["geographies"]

                    # Can be incorporated places or census designated places
                    if (placeData := data.get("Incorporated Places")) is None:
                        if (placeData := data.get("Census Designated Places")) is None:
                            # No geography found
                            continue
                            
                    state_id = placeData[0]["STATE"]
                    place_id = placeData[0]["PLACE"]

                    async with places_lock:
                        if (city, state) not in places_done:
                            places_done.add((city, state)) # Avoid race condition potential duplicate calls/data
                            places_not_saved[(city, state)] = (state_id, place_id)

                if (num_done % 1000) == 0: # Save to file occasionally so can recover
                    async with places_lock:
                        with open(SAVE_FILE_PATH, 'a') as file:
                            for (city, state), (state_id, place_id) in places_not_saved.items():
                                file.write(f"{city},{state},{state_id},{place_id}\n")
                        places_not_saved = {}
                        save_finished_idx(num_done)
        # Save rest
        with open(SAVE_FILE_PATH, 'a') as file:
            for (city, state), (state_id, place_id) in places_not_saved.items():
                file.write(f"{city},{state},{state_id},{place_id}\n")
            places_not_saved = {}
            save_finished_idx(num_done)
        print("Done Geocoding Places")

    async def main():
        df = pd.read_json("Data/Yelp JSON/yelp_academic_dataset_business.json", lines=True) # type: ignore

        # states either not in US, or manually checked the 1-2 businesses to be wrong
        bad_states = set(["AB", "HI", "UT", "SD", "XMS", "VI"]) 
        df = df[["city", "state", "latitude", "longitude"]]
        df = df[~df["state"].isin(bad_states)]
        df["city"] = df["city"].apply(clean_city_name)
        start_from_beginning = input("Start from beginning of Yelp Data (y/[n])? ").lower() == 'y'
        print()

        finished = False
        while not finished:
            try:
                await geocode(df, start_from_beginning)
                set_key(DOTENV_PATH, "GEOCODE_FINISHED_ALL", "1", quote_mode="never")
                finished = True
            except HttpBadRequest as e:
                print(f"HTTP Request Error:\n{e}\nRetrying in 30s:")
                start_from_beginning = False
                await asyncio.sleep(30)
            except Exception as e:
                print(f"{e}\nRetrying in 30s:")
                start_from_beginning = False
                await asyncio.sleep(30)

    
    asyncio.run(main())
