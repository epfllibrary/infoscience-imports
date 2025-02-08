document.getElementById("pipeline-form").addEventListener("submit", function (event) {
    event.preventDefault();

    let formData = new FormData(this);

    fetch("/start_pipeline", {
      method: "POST",
      body: formData,
    })
      .then((response) => response.json())
      .then((data) => {
        document.getElementById(
          "status-message"
        ).innerHTML = `<div class='alert alert-success'>${data.message}</div>`;
        updateButtons(true); // Disable start, enable stop
        startLogStream();
        checkForReport();
      })
      .catch((error) => {
        document.getElementById(
          "status-message"
        ).innerHTML = `<div class='alert alert-danger'>Error starting pipeline.</div>`;
      });
  });


document.getElementById("stop-pipeline").addEventListener("click", function () {
  fetch("/stop_pipeline", { method: "POST" })
    .then((response) => response.json())
    .then((data) => {
      document.getElementById(
        "status-message"
      ).innerHTML = `<div class='alert alert-warning'>${data.message}</div>`;
      updateButtons(false); // Re-enable start, disable stop
    })
    .catch((error) => {
      console.error("Error stopping pipeline.");
    });
});

// Function to update button states
function updateButtons(isRunning) {
  document.getElementById("start-pipeline").disabled = isRunning;
  document.getElementById("stop-pipeline").disabled = !isRunning;
}

// Function to check if the pipeline is running
function checkPipelineStatus() {
  fetch("/check_status")
    .then((response) => response.json())
    .then((data) => {
      updateButtons(data.running);
      if (data.running) {
        startLogStream();
      }
    })
    .catch((error) => console.error("Error checking pipeline status."));
}

// Function to listen for real-time logs
function startLogStream() {
  const logContainer = document.getElementById("log-container");
  logContainer.innerHTML = ""; // Clear previous logs

  const eventSource = new EventSource("/stream_logs");

  eventSource.onmessage = function (event) {
    const logMessage = document.createElement("p");
    logMessage.textContent = event.data;
    logContainer.appendChild(logMessage);
    logContainer.scrollTop = logContainer.scrollHeight; // Auto-scroll
  };

  eventSource.onerror = function () {
    console.error("Log stream error.");
    eventSource.close();
  };
}

// Function to check if the report is available
function checkForReport() {
  fetch("/check_report")
    .then((response) => response.json())
    .then((data) => {
      if (data.report_available) {
        document.getElementById(
          "report-section"
        ).innerHTML = `<a href="/download_report" class="btn btn-success mt-3">Download Report (${data.report_name})</a>`;
      } else {
        setTimeout(checkForReport, 5000); // Check again in 5 seconds
      }
    })
    .catch((error) => {
      console.error("Error checking report availability.");
    });
}

// Run the status check on page load
window.onload = checkPipelineStatus;
