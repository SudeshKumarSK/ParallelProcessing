import asyncio
import time
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from functools import partial

# =================================================== Asynchronous I/O-bound and CPU-bound tasks Start Here ===================================================
# ------------------------------
# Simulate I/O-bound Data Load
# ------------------------------
async def load_large_dataframe():
    print("[I/O] Starting to load large DataFrame...")
    await asyncio.sleep(3)  # Simulate I/O wait (e.g., disk or network)
    
    # Simulated large DataFrame
    df = pd.DataFrame({
        "id": [123, 456, 789],
        "value": ["Test1", "Test2", "Test3"]
    })
    print("[I/O] Finished loading DataFrame.")
    return df

# ------------------------------
# Simulate CPU-bound ML Inference
# ------------------------------
def run_inference_on_data(n):
    print(f"[CPU] Starting heavy ML inference for {n} samples...")
    predictions = {i: float(i % 2) for i in range(n)}  # Dummy binary predictions
    
    # Simulating an expensive computation ...
    for _ in range(n): pass
    
    print("[CPU] Heavy ML Model Inference completed, predictions are ready!")
    return predictions

async def cpu_inference_async(n):
    print("[CPU] Starting async CPU-bound task...")

    # Create a partially-applied function
    inference_func = partial(run_inference_on_data, n)

    with ProcessPoolExecutor() as executor:
        result = await asyncio.get_event_loop().run_in_executor(executor, inference_func)

    print("[CPU] Async CPU-bound task complete.")
    return result

# ------------------------------
# Async Progress Logger
# ------------------------------
async def progress_logger(n_ticks=5):
    for i in range(n_ticks):
        print(f"[Async] Progress tick {i+1}")
        await asyncio.sleep(1)


# =================================================== Synchronous Blocking I/O-bound and CPU-bound tasks End Here ===================================================
# Simulate I/O-bound data load
def load_large_dataframe_sync():
    print("[I/O] Starting to load large DataFrame...")
    time.sleep(3) 
    df = pd.DataFrame({
        "id": [123, 456, 789],
        "value": ["Test1", "Test2", "Test3"]
    })
    print("[I/O] Finished loading DataFrame.")
    return df

# Simulate CPU-bound ML inference
def run_inference_sync(n):
    print(f"[CPU] Starting heavy ML inference for {n} samples...")
    predictions = {i: float(i % 2) for i in range(n)}
    for _ in range(n): pass 
    print("[CPU] Inference complete.")
    return predictions

# Simulate progress logging
def progress_logger_sync(n_ticks=5):
    for i in range(n_ticks):
        print(f"[Sync] Progress tick {i+1}")
        time.sleep(1)  # Blocking


# ------------------------------
# Main orchestrators for sync and async
# ------------------------------
def main_sync():
    print("\n Starting synchronous tasks...\n")
    
    df = load_large_dataframe_sync()        # Blocking I/O
    predictions = run_inference_sync(100_000_000)  # Blocking CPU
    progress_logger_sync(n_ticks=10)             # Blocking logging

    print("\n All synchronous tasks complete.")
    print(f"[Result] DataFrame shape: {df.shape}")
    print(f"[Result] ML Predictions: {list(predictions.items())[:5]}...")
    print(f"[Result] Total predictions: {len(predictions)}")


async def main():

    print("\n Starting all tasks...\n")

    # Start all tasks concurrently
    tasks = await asyncio.gather(
        load_large_dataframe(),
        cpu_inference_async(100_000_000),  # Simulate 100,000 samples
        progress_logger(n_ticks=10)
    )

    df_result, ml_predictions, _ = tasks

    print("\nAll tasks have been completed!!!")
    print(f"[Result] DataFrame shape: {df_result.shape}")
    print(f"[Result] ML Predictions: {list(ml_predictions.items())[:5]}...")  # Show first 5 predictions
    print(f"[Result] ML Predictions length: {len(ml_predictions)}")


if __name__ == "__main__":
    start_time = time.time()
    print("Starting the main function...")
    asyncio.run(main())
    end_time = time.time()

    print("===============================================================================")
    print(f"Total time taken: {end_time - start_time:.2f} seconds for asynchronous tasks.")
    print("===============================================================================")
    print()

    print("Starting the synchronous main function...")
    start_time = time.time()
    main_sync()
    end_time = time.time()

    print("===============================================================================")
    print(f"Total time taken: {end_time - start_time:.2f} seconds for synchronous tasks.")
    print("===============================================================================")
    print()






