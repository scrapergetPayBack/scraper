import asyncio
from fastapi import FastAPI
from fastapi import APIRouter
import uvicorn
import database
from bson import ObjectId
import json
import os

# Create a router instance
router = APIRouter()



def update_scraper_state(running=None, paused=None):
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    STATE_FILE = os.path.join(BASE_DIR, "scraper_state.json")
    with open(STATE_FILE, "r") as f:
        state = json.load(f)

    if running is not None:
        state["running"] = running
    if paused is not None:
        state["paused"] = paused

    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def get_scraper_state():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    STATE_FILE = os.path.join(BASE_DIR, "scraper_state.json")
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
    return state





@router.get("/v1")
async def root():
    return {"message": "Welcome to the getpayback.com api"}





@router.get("/v1/shop_info")
async def scrape_shop(shop_domain: str):
    """
    Endpoint to scrape shop information
    Parameters:
    - shop_domain: The shop's domain for internal tracking.
    """
    try:
        result = await database.find_shop_by_domain(shop_domain)
        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/v1/product")
async def scrape_shop(shop_domain: str, title: str, title2: str):

    """
    Endpoint to scrape shop information
    Parameters:
    - shop_domain: The shop's domain for internal tracking.
    - title: The title of the product.
    - title2: The additional title of the product.
    """
    try:
        # Fetch the shop ID
        shop_id = await database.find_shop_id_by_domain(shop_domain)
        
        # Convert ObjectId to string if necessary
        shop_id_str = str(shop_id)

        # Find the product based on shop ID, title, and title2
        result = await database.find_product(shop_id_str, title, title2)

        # If `result` contains ObjectId fields, convert them too
        if result and isinstance(result, dict):
            result["_id"] = str(result["_id"])  # Convert _id to string
            if "shop_id" in result:  # Convert shop_id if it exists
                result["shop_id"] = str(result["shop_id"])

        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    










@router.post("/v1/scraper/pause")
async def pause_scraper():
    scraper_state = get_scraper_state()
    if not scraper_state["running"]:
        return {"status": "error", "message": "Cannot pause. Scraper is not running."}
    if scraper_state["paused"]:
        return {"status": "error", "message": "Scraper is already paused. But scraper is still runnnig"}

    update_scraper_state(running=True, paused=True)
    return {"status": "success", "message": "Scraper paused."}

@router.post("/v1/scraper/unpause")
async def unpause_scraper():
    scraper_state = get_scraper_state()
    if not scraper_state["running"]:
        return {"status": "error", "message": "Cannot unpause. Scraper is not running."}
    if not scraper_state["paused"]:
        return {"status": "error", "message": "Scraper is not paused."}

    update_scraper_state(running=True,paused=False)
    return {"status": "success", "message": "Scraper unpaused."}

@router.post("/v1/scraper/stop")
async def stop_scraper():
    scraper_state = get_scraper_state()
    if not scraper_state["running"]:
        return {"status": "error", "message": "Scraper is not running."}

    update_scraper_state(running=False, paused=False)
    return {"status": "success", "message": "Scraper stopped."}

@router.post("/v1/scraper/start")
async def start_scraper():
    scraper_state = get_scraper_state()
    if scraper_state["running"]:
        return {"status": "error", "message": "Scraper is already running."}

    update_scraper_state(running=True, paused=False)
    return {"status": "success", "message": "Scraper started."}






# Create the FastAPI app
app = FastAPI()

# Include the API router
app.include_router(router)


async def run_api_server():
    """
    Run the FastAPI server using Uvicorn.
    """
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()



async def main2():
    """
    Main entry point for running both the scraper and the API server concurrently.
    """
    # Run the scraper and the API server concurrently
    await run_api_server(),    # API server



if __name__ == "__main__":
    asyncio.run(main2())