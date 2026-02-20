import os
import time
import subprocess
import sys

def main():
    if len(sys.argv) < 3:
        print("Usage: python run_reports.py <start_index> <end_index>")
        sys.exit(1)
        
    start_idx = int(sys.argv[1])
    end_idx = int(sys.argv[2])
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Get all python files except this one
    all_scripts = sorted([
        f for f in os.listdir(script_dir) 
        if f.endswith('.py') and f != 'run_reports.py'
    ])
    
    # Get the specific batch to run
    batch = all_scripts[start_idx:end_idx]
    
    if not batch:
        print(f"No scripts found in range {start_idx} to {end_idx}.")
        return

    for i, script in enumerate(batch):
        script_path = os.path.join(script_dir, script)
        global_idx = start_idx + i
        
        print("=" * 60)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Executing {script} (Script {global_idx + 1} of {len(all_scripts)})")
        print("=" * 60)
        
        try:
            # We run using the same python interpreter
            subprocess.run([sys.executable, script_path], check=True)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Successfully finished {script}")
        except subprocess.CalledProcessError as e:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Error executing {script}: {e}")
            # We continue with the next script even if one fails
            
        # Wait 15 minutes (900 seconds) before the next execution.
        # Skip waiting if it is the absolute last script in the entire list.
        if global_idx < len(all_scripts) - 1:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Waiting for 15 minutes before the next execution...")
            # For testing, you could comment out time.sleep(15 * 60) and use time.sleep(5)
            time.sleep(15 * 60)

if __name__ == "__main__":
    main()
