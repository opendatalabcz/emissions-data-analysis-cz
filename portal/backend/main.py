import asyncio
import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from preprocess import run_preprocessing
from predict import infer_vin, init_model, build_vin_index
import sys
import config

is_ready = False

app = FastAPI()

@app.get("/predict")
async def predict_risk(vin: str):
    """
    Endpoint, který zavolá Next.js frontend po zadání VINu.
    """
    try:
        return infer_vin(vin)
    except Exception as e:
        return {"error": f"Při zpracování predikce nastala chyba: {str(e)}"}

@app.get("/graphs_json")
async def get_graphs_json():
    file_path = "/app/public/graphs/available_months.json"
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return []

@app.get("/graphs_svg")
async def get_graphs_svg(file: str):
    if ".." in file or "/" in file:
        return {"error": "Neplatný název souboru"}
    file_path = f"/app/public/graphs/{file}"
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"error": "Soubor nebyl nalezen"}

@app.get("/health")
async def health_check():
    if is_ready:
        return {"status": "ok"}
    raise HTTPException(status_code=503, detail="Server se inicializuje a stahuje nová data.")

async def update_data_and_generate_graphs():
    global is_ready
    while True:
        print("Stahuji data z data.gov.cz do /app/data/parquets...")
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", "from preprocess import run_preprocessing; run_preprocessing()"
        )
        await process.wait()
        
        print("Generuji grafy z .parquet souborů...", flush=True)
        process_graphs = await asyncio.create_subprocess_exec(
            sys.executable, "-c", "from generate_graphs import generate_all_graphs; generate_all_graphs()"
        )
        await process_graphs.wait()
        
        print("Aktualizuji index VIN v paměti...", flush=True)
        await asyncio.to_thread(build_vin_index)
        print("Aktualizace indexu dokončena.", flush=True)
        
        print(f"Grafy uloženy. Uspávám na {config.UPDATE_INTERVAL_DAYS} dní.", flush=True)
        is_ready = True
        await asyncio.sleep(config.UPDATE_INTERVAL_DAYS * 86400)

@app.on_event("startup")
async def startup_event():
    init_model()
    asyncio.create_task(update_data_and_generate_graphs())
