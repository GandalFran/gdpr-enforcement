import os
import subprocess
import json
import time
import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("runner")

EXPERIMENTS_DIR = "code/experiments"
SRC_DIR = "code/src"

def start_service(command, name, output_dir, env=None):
    logger.info(f"Starting {name}...")
    log_path = os.path.join(output_dir, f"{name}.log")
    log_file = open(log_path, "w")
    p = subprocess.Popen(command, stdout=log_file, stderr=subprocess.STDOUT, shell=False, env=env)
    return p, log_file

def run_experiment_script(script_name, args=[], output_dir=None, env=None):
    cmd = ["python", f"{EXPERIMENTS_DIR}/{script_name}"] + args
    logger.info(f"Running {script_name}...")
    
    if output_dir:
        log_path = os.path.join(output_dir, f"{script_name}.log")
        with open(log_path, "w") as log_file:
            try:
                subprocess.run(cmd, check=True, stdout=log_file, stderr=subprocess.STDOUT, env=env)
                logger.info(f"{script_name} completed. Logs in {log_path}")
            except subprocess.CalledProcessError as e:
                logger.error(f"{script_name} failed: {e}. Check {log_path}")
    else:
        try:
            subprocess.run(cmd, check=True, env=env)
            logger.info(f"{script_name} completed.")
        except subprocess.CalledProcessError as e:
            logger.error(f"{script_name} failed: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-infra", action="store_true", help="Skip bringing up docker infra")
    parser.add_argument("--docker-mode", action="store_true", help="Run in docker mode (assume services running)")
    parser.add_argument("--experiment-name", default="default", help="Name of the experiment")
    args = parser.parse_args()
    
    # Define Metrics Output Directory
    metrics_root = "code/metrics"
    experiment_dir = os.path.join(metrics_root, args.experiment_name)
    os.makedirs(experiment_dir, exist_ok=True)
    
    logger.info(f"Starting experiment: {args.experiment_name}")
    logger.info(f"Output directory: {experiment_dir}")

    # 1. Start Infrastructure (Optional - Docker)
    if not args.skip_infra and args.docker_mode:
        logger.info("Starting Docker Infrastructure...")
        subprocess.run(["docker-compose", "-f", "code/docker/docker-compose.yml", "up", "-d"], check=True)
        logger.info("Waiting 30s for Kafka/Postgres to be ready...")
        time.sleep(30)
    
    procs = []
    files = []
    
    try:
        if not args.docker_mode:
            # Local Orchestration Mode
            logger.info("Running in Local Mode. Starting services...")
            
            # Prepare Environment
            env = os.environ.copy()
            env["PYTHONPATH"] = os.path.join(os.getcwd(), "code")
            # Set DB Path for API to avoid contention with background process
            env["SQLITE_DB_PATH"] = os.path.join(experiment_dir, "audit_log.db")
            
            # Start Audit API
            # Note: We assume running from root, so module is src.audit.api (added code to pythonpath)
            api_proc, api_log = start_service(
                [sys.executable, "-m", "uvicorn", "src.audit.api:app", "--host", "0.0.0.0", "--port", "8005"], 
                "audit_api", 
                experiment_dir,
                env=env
            )
            procs.append(api_proc)
            files.append(api_log)
            
            logger.info("Waiting 5s for API startup...")
            time.sleep(5)
            
            os.environ["AUDIT_API_URL"] = "http://localhost:8005"

        else:
            logger.info("Running in Docker Mode. Assuming services are up.")
            os.environ["AUDIT_API_URL"] = "http://127.0.0.1:8000"
            time.sleep(15) 

        # 4. Run Experiments
        logger.info(">>> Running Audit Scenarios (S1-S3)...")
        run_experiment_script("audit_scenarios.py", ["--scenario", "all", "--output-dir", experiment_dir], output_dir=experiment_dir, env=env)
        
        logger.info(">>> Running RoPA Scenario (S4)...")
        run_experiment_script("ropa_scenario.py", ["--output-dir", experiment_dir], output_dir=experiment_dir, env=env)
        
        if not args.docker_mode:
             logger.info(">>> Running Performance Benchmark (Low Load for Dev)...")
             run_experiment_script("performance_benchmark.py", ["--output-dir", experiment_dir, "--count", "100"], output_dir=experiment_dir, env=env)
        else:
             logger.info(">>> Running Performance Benchmark (Medium Load for Docker)...")
             run_experiment_script("performance_benchmark.py", ["--output-dir", experiment_dir, "--count", "1000"], output_dir=experiment_dir, env=env)
        
        logger.info(">>> Running Accountability checks...")
        run_experiment_script("accountability_check.py", ["--output-dir", experiment_dir], output_dir=experiment_dir, env=env)
        
        logger.info(f"Experiment {args.experiment_name} completed. Check {experiment_dir}/*.json.")
        
        # Aggregate results for user convenience (similar to fabricate_results but real)
        # We can write a summary if needed.

    except Exception as e:
        logger.error(f"Runner failed: {e}")
    finally:
        if not args.docker_mode:
            logger.info("Stopping services...")
            for p in procs:
                p.terminate()
            for f in files:
                f.close()

if __name__ == "__main__":
    main()
