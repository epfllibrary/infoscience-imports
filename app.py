'''Lightweight Flask app to start the data pipeline.'''

import os
import time
import glob
import threading
import signal
from flask import Flask, render_template, request, jsonify, Response, send_file

from data_pipeline.main import main

app = Flask(__name__)

LOG_DIR = "logs"  # Directory where logs are stored
DATA_DIR = "data"  # Directory where reports are saved
pipeline_thread = None  # Track the running pipeline thread
pipeline_running = False  # Track if the pipeline is running


def get_latest_log_file():
    """Returns the latest log file from the logs directory."""
    log_files = sorted(
        glob.glob(os.path.join(LOG_DIR, "*.log")), key=os.path.getmtime, reverse=True
    )
    return log_files[0] if log_files else None


def get_latest_execution_folder():
    """Returns the latest timestamped execution folder."""
    subdirs = sorted(
        [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))],
        key=lambda x: os.path.getmtime(os.path.join(DATA_DIR, x)),
        reverse=True,
    )
    return os.path.join(DATA_DIR, subdirs[0]) if subdirs else None


def get_latest_report():
    """Returns the latest generated Excel report from the newest execution folder."""
    latest_folder = get_latest_execution_folder()
    if not latest_folder:
        return None

    report_files = glob.glob(os.path.join(latest_folder, "*.xlsx"))
    return report_files[0] if report_files else None


def run_pipeline(start_date, end_date, custom_queries=None, author_ids=None):
    """Runs the pipeline and updates the process state."""
    global pipeline_running
    pipeline_running = True
    main(start_date, end_date, queries=custom_queries, authors_ids=author_ids)
    pipeline_running = False


@app.route("/")
def index():
    """Render the main UI."""
    return render_template("index.html")


@app.route("/start_pipeline", methods=["POST"])
def start_pipeline():
    """Starts the data pipeline in a separate thread if not already running."""
    global pipeline_thread, pipeline_running

    if pipeline_running:
        return jsonify({"message": "Pipeline is already running!", "running": True})

    start_date = request.form.get("start_date", "2025-01-01")
    end_date = request.form.get("end_date", "2026-01-01")

    # Retrieve custom queries
    custom_wos_query = request.form.get("custom_wos_query", "").strip()
    custom_scopus_query = request.form.get("custom_scopus_query", "").strip()

    # Retrieve author IDs as a list
    author_ids_raw = request.form.get("author_ids", "").strip()
    author_ids = [
        author_id.strip()
        for author_id in author_ids_raw.split(",")
        if author_id.strip()
    ]

    # Prepare custom queries dictionary
    custom_queries = {}
    if custom_wos_query:
        custom_queries["wos"] = custom_wos_query
    if custom_scopus_query:
        custom_queries["scopus"] = custom_scopus_query

    pipeline_thread = threading.Thread(
        target=run_pipeline, args=(start_date, end_date, custom_queries, author_ids)
    )
    pipeline_thread.start()

    return jsonify({"message": "Pipeline started successfully!", "running": True})


@app.route("/stop_pipeline", methods=["POST"])
def stop_pipeline():
    """Stops the pipeline process."""
    global pipeline_running, pipeline_thread

    if not pipeline_running:
        return jsonify({"message": "No pipeline is running.", "running": False})

    pipeline_running = False  # Flag as stopped
    os.kill(
        os.getpid(), signal.SIGTERM
    )  # Forcefully terminate Flask (or replace with a better cleanup strategy)

    return jsonify({"message": "Pipeline stopped.", "running": False})


@app.route("/check_status", methods=["GET"])
def check_status():
    """Returns whether the pipeline is running."""
    return jsonify({"running": pipeline_running})


@app.route("/stream_logs")
def stream_logs():
    """Stream log file content to the frontend."""

    def generate():
        latest_log = get_latest_log_file()
        if not latest_log:
            yield "data: No log file found.\n\n"
            return

        with open(latest_log, "r") as f:
            f.seek(0, os.SEEK_END)
            while pipeline_running:
                line = f.readline()
                if line:
                    yield f"data: {line.strip()}\n\n"
                time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream")


@app.route("/check_report", methods=["GET"])
def check_report():
    """Checks if a report exists and returns its name."""
    latest_report = get_latest_report()
    if latest_report:
        return jsonify(
            {"report_available": True, "report_name": os.path.basename(latest_report)}
        )
    return jsonify({"report_available": False})


@app.route("/download_report", methods=["GET"])
def download_report():
    """Serves the latest report file for download."""
    latest_report = get_latest_report()
    if latest_report:
        return send_file(latest_report, as_attachment=True)
    return "No report available", 404


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
