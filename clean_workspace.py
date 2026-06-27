import os
import shutil

def clean_workspace():
    print("=============================================")
    print("Starting Workspace Cleanup Utility...")
    print("=============================================\n")

    # 1. Directories/Files to remove completely
    paths_to_remove = [
        # Python & Pytest caches
        ".pytest_cache",
        
        # Local Spark warehouse
        "data",
        
        # Redundant virtual environment (active is 'venv')
        ".venv",
        
        # Generated runtime telemetry logs (keeping 'RCA_Logging_Artifacts/raw'!)
        "RCA_Logging_Artifacts/logs",
        "RCA_Logging_Artifacts/quarantine",
        "RCA_Logging_Artifacts/repairs",
        "RCA_Logging_Artifacts/bronze",
        "RCA_Logging_Artifacts/silver",
        "RCA_Logging_Artifacts/gold",
    ]

    for path in paths_to_remove:
        abs_path = os.path.abspath(path)
        if os.path.exists(abs_path):
            print(f"Removing: {path}...")
            try:
                if os.path.isdir(abs_path):
                    shutil.rmtree(abs_path)
                else:
                    os.remove(abs_path)
                print(f"  -> Successfully removed.")
            except Exception as e:
                print(f"  -> Error removing {path}: {e}")

    # 2. Recursively remove __pycache__ directories
    print("\nScanning for __pycache__ directories...")
    pycache_count = 0
    for root, dirs, files in os.walk(".", topdown=False):
        # Exclude active venv
        if "venv" in root:
            continue
        for name in dirs:
            if name == "__pycache__":
                pycache_path = os.path.join(root, name)
                try:
                    shutil.rmtree(pycache_path)
                    pycache_count += 1
                except Exception as e:
                    print(f"Error removing {pycache_path}: {e}")
    print(f"Removed {pycache_count} __pycache__ directories.")

    print("\n=============================================")
    print("Workspace cleanup complete!")
    print("=============================================")

if __name__ == "__main__":
    clean_workspace()
