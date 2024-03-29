# Pyspark Volleyball Data Processing Project
The data file (data-volleyball.csv) in csv format contains the results of volleyball matches. The processing of this data should be done using Apache Spark and specifically pyspark. The task consists of three tasks:

### Task 1: Basic Data Processing (data_processing.py)

1. For the following columns in the file, remove the percentage symbol from the values and display the result on the screen without truncation.
percentage_cols = ['T1_Srv_Eff', 'T1_Rec_Pos', 'T1_Rec_Perf', 'T1_Att_Kill_Perc', 'T1_Att_Eff', 'T1_Att_Sum', 'T2_Srv_Eff', 'T2_Rec_Pos', 'T2_Rec_Perf', 'T2_Att_Kill_Perc', 'T2_Att_Eff', 'T2_Att_Sum']

2. Convert the names of the groups to capital letters.

3. Calculate and store in a variable the number of matches in the file you read.

4. Calculate the total number of sets conducted per match.

5. Calculate and save to a csv file the number of matches per number of total sets.

6. Calculate and save in a csv file the number of matches played by each team (Attention! A team can be either home or away).


### Task 2: create a table of statistics (data_team_statistics.py)

Create a table of statistics for all the teams shown in the csv file. This table include, for each of the teams:

- The matches (home, away and total) that the team has played.
- The total sets lost and won by each team.
- The total points lost and won by each team.
- These statistics is sorted in descending order of total matches won for each team.

### Task 3: Using the Apache Spark Structured Streaming API (data_streaming.py)

For the data to be saved from the web application, using Apache Spark Structured Streaming, place the resulting csv file from the web application in an appropriate folder, update the statistics table of Task 2 with this mechanism, and save the new statistics table in a csv file.

# CSV Files Instructions
### The CSV file use as input the following fields (with corresponding T1 and T2 for the home and away team).
- Date - Date and time the match was played
- Team_1 & Team_2 - Name of the teams
- Score - Number of sets won
- Sum - Total number of points won
- BP - Points scored on a counter-attack with her own serve
- Ratio - Points won - points lost
- Srv_Sum - Number of all serves
- Srv_Err - Number of service errors
- Srv_Ace - Number of points won by an ace
- Srv_Eff - Service efficiency in %
- Rec_Sum - Number of all service receptions
- Rec_Err - Number of service reception errors
- Rec_Pos - Percentage of positive service receptions
- Rec_Perf - Percentage of perfect service slots
- Att_Sum - Total number of attacks
- Att_Err - Number of attack errors
- Att_Blk - Number of attacks blocked
- Att_Kill - Total number of points scored in the attack
- Att_Kill_Perc - Percentage of attacks that scored a point
- Att_Eff - Attack effectiveness in %.
- Blk_Sum - Number of points won with a block
- Blk_As - Number of blocks allowing a blocking team to counter-attack
- Winner - 0 if team 1 won, 1 if team 2 won

### The csv file that web application produces has the following fields
- HomeTeam: Name of the home team
- AwayTeam: Name of the away team
- HomeSets: Winning sets of home team
- AwaySets: Winning sets of away team
- HomePoints: Total points won by home team
- AwayPoints: Total points won by the away team

# Execution Instructions

### Task 1:
- Execute the following command in the terminal:
spark-submit
/home/bigdata/PycharmProjects/sparkVolleyball/data_processing.py
--deploy-mode client --py-files
/home/bigdata/PycharmProjects/sparkVolleyball/data_processing.py
### Task 2:
- Execute the following command in the terminal:
spark-submit
/home/bigdata/PycharmProjects/sparkVolleyball/data_team_statistics.py
--deploy-mode client --py-files
/home/bigdata/PycharmProjects/sparkVolleyball/data_team_statistics.py
### Task 3:
- Start HDFS on the terminal
- Open a new terminal and run nc -lk 9999
- Open a new terminal and run the command:
-park-submit --master local[*].
/home/bigdata/PycharmProjects/sparkVolleyball/data_streaming.py
localhost 9999
- Download statistics from front-end

### **NOTE**: 
1. Absolute paths are used
2. Project was run on Linux

# Volleyball Scoring Application

This application enables users to score volleyball matches between two teams. Users can input team names, track scores, sets, serving team, and activate timeouts. The timeout feature is triggered automatically when the leading team reaches 8 or 16 points. The application provides real-time updates on the scoreboard and enables users to download match statistics in CSV format.


# Data Streaming Example:

1. Start HDFS
   
   ![Start HDFS](images/step_1.jpg)
2. Start Server
   
   ![Start Server](images/step_2.jpg)
3. Submit Python Script
   
   ![python submit script](images/step_3.jpg)
4. Download Statistics from Front-End
   
   The first downloaded file include a CSV with the below fields:
   - HomeTeam: ZAKSA Kędzierzyn-Koźle
   - AwayTeam: PGE Skra Bełchatów
   - HomeSets: 3
   - AwaySets: 1
   - HomePoints: 80
   - AwayPoints: 56
     
   Waiting for a while and the updated table will show:
   
   ![first_download](images/step_4.jpg)
5. Download Second Game Statistics from Front-End
    
   - HomeTeam: Asseco Resovia
   - AwayTeam: Jastrzębski Węgiel
   - HomeSets: 3
   - AwaySets: 2
   - HomePoints: 87
   - AwayPoints: 78
     
   ![second_download](images/step_5.jpg)
