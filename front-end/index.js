// Variable Initialization
let teamAName = prompt("Enter the name for Team A") || "Team A";
let teamBName = prompt("Enter the name for Team B") || "Team B";
let teamAScore = 0;
let teamBScore = 0;
let teamASets = 0;
let teamBSets = 0;
let servingTeam = null;
let timeoutActive = false;
let timeoutSeconds = 60;
let timeoutTimer;
let gameEnded = false;
let teamATotalScore = 0;
let teamBTotalScore = 0;
let timeoutButton = document.getElementById("buttonTimeout");
let teamAscoreButton = document.getElementById("buttonScoreA");
let teamBscoreButton = document.getElementById("buttonScoreB");
let winner = document.getElementById("winner");

// Function for checking the end of the game
function checkGameEnd() {
  if (teamASets === 3 && teamBSets < 3) {
    displayWinner(teamAName);
    gameEnded = true;
    disableButtonsAfterGame();
  } else if (teamBSets === 3 && teamASets < 3) {
    displayWinner(teamBName);
    gameEnded = true;
    disableButtonsAfterGame();
  }
}

function disableButtonsAfterGame() {
  teamAscoreButton.disabled = true;
  teamBscoreButton.disabled = true;
  timeoutButton.disabled = true;
}

function enableButtons() {
  teamAscoreButton.disabled = false;
  teamBscoreButton.disabled = false;
  timeoutButton.disabled = false;
}

// Function for displaying the winner
function displayWinner(teamName) {
  winner.innerHTML =
    "Winner: <span style='color: red; font-weight: bold;'>" +
    teamName +
    "</span>";
  alert(`${teamName} wins the game!`);
}

// Function for updating the scoreboard
function updateScoreboard() {
  document.getElementById(
    "teamA"
  ).innerHTML = `${teamAName}<br />Sets: ${teamASets}`;
  document.getElementById("score").innerHTML = `${teamAScore} - ${teamBScore}`;
  document.getElementById(
    "teamB"
  ).innerHTML = `${teamBName}<br />Sets: ${teamBSets}`;
  document.getElementById("currentServing").innerHTML = `Serving: ${
    servingTeam ? servingTeam : "-"
  }`;
}

// Function for adding points to a team
function addPoint(team) {
  if (timeoutActive) {
    alert("Cannot score during a timeout!");
    return;
  }

  if (team === "A") {
    teamAScore++;
    teamATotalScore++;
  } else if (team === "B") {
    teamBScore++;
    teamBTotalScore++;
  }

  adjustSetsAfterScoring();
  checkGameEnd();

  updateServingTeamName(team);
  updateScoreboard();
  checkPointsForTimeout(team);
}

function updateServingTeamName(team) {
  if (team === "A") {
    servingTeam = teamAName;
  } else {
    servingTeam = teamBName;
  }
}

// Function for adjusting sets after scoring
function adjustSetsAfterScoring() {
  // Check if a team has reached 25 points with a difference of at least 2
  if (
    (teamAScore >= 25 || teamBScore >= 25) &&
    Math.abs(teamAScore - teamBScore) >= 2
  ) {
    // Update sets based on the winner of the current round
    if (teamAScore > teamBScore) {
      teamASets++;
    } else {
      teamBSets++;
    }

    // Reset individual scores for the next round if game not ended
    if (!(teamASets === 3 || teamBSets === 3)) {
      teamAScore = 0;
      teamBScore = 0;
    }
  }

  // Check if both teams have 2 sets and decide the winner based on the next round
  if (teamASets === 2 && teamBSets === 2) {
    // Check if a team has reached 15 points with a difference of at least 2 in the 5th set
    if (
      (teamAScore >= 15 || teamBScore >= 15) &&
      Math.abs(teamAScore - teamBScore) >= 2
    ) {
      // Update sets based on the winner of the 5th set
      if (teamAScore > teamBScore) {
        teamASets++;
      } else {
        teamBSets++;
      }
    }
  }
}

// Function for checking points for timeout
function checkPointsForTimeout(team) {
  if (teamAScore > teamBScore && (teamAScore === 8 || teamAScore === 16)) {
    if (team !== "B") {
      activateTimeout();
    }
  } else if (
    teamBScore > teamAScore &&
    (teamBScore === 8 || teamBScore === 16)
  ) {
    if (team !== "A") {
      activateTimeout();
    }
  }
}

// Function for activating the timeout
function activateTimeout() {
  if (timeoutActive) {
    alert("Timeout is already active!");
    return;
  }

  resetTimeout(); // Reset timeout-related variables

  timeoutActive = true;
  updateTimeoutLabel();

  timeoutTimer = setInterval(function () {
    if (timeoutSeconds <= 0) {
      clearInterval(timeoutTimer);
      timeoutActive = false;
      timeoutSeconds = 60; // Reset timeoutSeconds to its initial value
      updateTimeoutLabel();
      updateTimeoutTimer();
      timeoutButton.classList.remove("timeout-triggered");
    } else {
      timeoutSeconds--;
      updateTimeoutTimer();
    }
  }, 1000);

  timeoutButton.classList.add("timeout-triggered");
}

// Function for resetting the timeout
function resetTimeout() {
  clearInterval(timeoutTimer);
  timeoutActive = false;
  timeoutSeconds = 60;
  updateTimeoutLabel();
  updateTimeoutTimer();
}

// Function for updating the label of the timeout
function updateTimeoutLabel() {
  document.getElementById("timeoutLabel").innerHTML = timeoutActive
    ? "Timeout Remaining:"
    : "Timeout:";
}

// Function for updating the timer of the timeout
function updateTimeoutTimer() {
  const minutes = Math.floor(timeoutSeconds / 60);
  const seconds = timeoutSeconds % 60;
  document.getElementById("timeoutTimer").innerHTML = `${minutes}:${
    seconds < 10 ? "0" : ""
  }${seconds}`;
}

// Function for resetting the scoreboard to its initial state
function resetGame() {
  teamAName = prompt("Enter the name for Team A") || "Team A";
  teamBName = prompt("Enter the name for Team B") || "Team B";
  teamAScore = 0;
  teamATotalScore = 0;
  teamBTotalScore = 0;
  teamBScore = 0;
  teamASets = 0;
  teamBSets = 0;
  servingTeam = null;
  timeoutActive = false;
  timeoutSeconds = 60;
  winner.innerHTML = "Winner: ";
  gameEnded = false;
  clearInterval(timeoutTimer);
  updateScoreboard();
  updateTimeoutLabel();
  updateTimeoutTimer();
  enableButtons();
}

function downloadStatistics() {
  if (gameEnded === false) {
    return alert("To download statistics, game must end!");
  }

  // Get the data you want to include in the CSV
  var data = {
    HomeTeam: teamAName,
    AwayTeam: teamBName,
    HomeSets: teamASets,
    AwaySets: teamBSets,
    HomePoints: teamATotalScore,
    AwayPoints: teamBTotalScore,
  };

  // Create a header row with column names
  var headerRow = Object.keys(data).join(",") + "\n";

  // Create a data row with values
  var dataRow = Object.values(data).join(",") + "\n";

  // Combine header and data rows
  var csvContent = headerRow + dataRow;

  // Create a Blob with the CSV data
  var blob = new Blob([csvContent], { type: "text/csv" });

  // Create a link element to trigger the download
  var link = document.createElement("a");
  link.href = window.URL.createObjectURL(blob);
  link.download = "volleyball_statistics.csv";

  // Append the link to the document and trigger the download
  document.body.appendChild(link);
  link.click();

  // Remove the link from the document
  document.body.removeChild(link);
}

// Initialization of the application
updateScoreboard();
updateTimeoutLabel();
updateTimeoutTimer();
