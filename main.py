
import os
import runpy
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
# Get script name from env variable
script_name = os.getenv("SCRIPT_NAME")

if not script_name:
    print("ERROR: SCRIPT_NAME environment variable is not set.")
    sys.exit(1)

# Build the full path under /scripts
scripts_dir = Path(__file__).parent / "scripts"
target_script = scripts_dir / f"{script_name}.py"

if not target_script.exists():
    print(f"ERROR: Script '{script_name}.py' not found in {scripts_dir}")
    sys.exit(1)

print(f"Running script: {target_script}")
# Make sure /app is on sys.path so imports like `utility.*` work
sys.path.insert(0, str(Path(__file__).parent))

# Run the script as if it were the main program
runpy.run_path(str(target_script), run_name="__main__")