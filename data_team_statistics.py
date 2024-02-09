from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, count

def get_all_teams(df):
    # Extract distinct teams from both Team_1 and Team_2 using joins
    teams_from_team_1 = df.select("Team_1").distinct().withColumnRenamed("Team_1", "Team")
    teams_from_team_2 = df.select("Team_2").distinct().withColumnRenamed("Team_2", "Team")

    # Perform join to combine distinct teams
    all_teams_df = teams_from_team_1.join(teams_from_team_2, on="Team", how="full_outer")

    return all_teams_df

def get_wins(df):
    # Extract distinct teams from both Team_1 and Team_2
    all_teams_df = get_all_teams(df)

    # Get Home Wins
    team_1_wins = df.filter(col("Winner") == 0).groupBy("Team_1").agg(count(col("Winner")).alias("Game_Wins")) \
        .withColumnRenamed("Team_1", "Team")
    # Get Away Wins
    team_2_wins = (df.filter(col("Winner") == 1).groupBy("Team_2").agg(count(col("Winner")).alias("Game_Wins")) \
                   .withColumnRenamed("Team_2", "Team"))

    # Get total wins (Union and sum the wins)
    total_teams_wins = team_1_wins.union(team_2_wins).groupBy("Team").agg(sum("Game_Wins").alias("Game_Wins"))

    # Left outer join with all_teams_df to include teams with 0 wins
    total_teams_wins = all_teams_df.join(total_teams_wins, on="Team", how="left_outer") \
        .fillna(0, subset=["Game_Wins"])

    return total_teams_wins


def get_number_of_home_games(df):
    # Extract distinct teams from both Team_1 and Team_2
    all_teams_df = get_all_teams(df)

    # 1)a) Get number of Home Games for each team
    team_1_games = df.groupBy("Team_1").agg(count("Team_1").alias("Home_Games_Played")) \
        .withColumnRenamed("Team_1", "Team")

    # Left outer join with all_teams_df to include teams with 0 home games
    team_1_games = all_teams_df.join(team_1_games, on="Team", how="left_outer") \
        .fillna(0, subset=["Home_Games_Played"])
    return team_1_games


def get_number_of_away_games(df):
    # Extract distinct teams from both Team_1 and Team_2
    all_teams_df = get_all_teams(df)

    # 1)b) Get number of Away Games for each team
    team_2_games = df.groupBy("Team_2").agg(count("Team_2").alias("Away_Games_Played")) \
        .withColumnRenamed("Team_2", "Team")

    # Left outer join with all_teams_df to include teams with 0 away games
    team_2_games = all_teams_df.join(team_2_games, on="Team", how="left_outer") \
        .fillna(0, subset=["Away_Games_Played"])
    return team_2_games

def get_total_number_of_games(df_home, df_away):

    # Join the DataFrames
    combined_df = df_home.join(df_away, on="Team", how="left_outer")
    #combined_df = combined_df.distinct()

    # Aggregate total games played for each team
    total_teams_games = combined_df.groupBy("Team").agg(sum(col("Home_Games_Played") +
                                                            col("Away_Games_Played")).alias("Total_Games_Played"))
    return total_teams_games

def get_win_sets(df):
    # 2)a)
    # Get Home Teams Sets for each team
    team_1_sets = df.groupBy("Team_1").agg(sum("T1_Score").alias("T_Score")) \
        .withColumnRenamed("Team_1", "Team")
    # Get Away Teams Sets for each team
    team_2_sets = df.groupBy("Team_2").agg(sum("T2_Score").alias("T_Score")) \
        .withColumnRenamed("Team_2", "Team")

    # Get total win sets (Union and sum the sets)
    total_teams_sets = team_1_sets.union(team_2_sets).groupBy("Team").agg(sum("T_Score").alias("Sets_Won"))

    return total_teams_sets


def get_lost_sets(df):
    # 2)b)
    # Lost Sets for every team
    team_1_lost_sets = df.groupBy("Team_1").agg(sum("T2_Score").alias("T_Score")) \
        .withColumnRenamed("Team_1", "Team")

    team_2_lost_sets = df.groupBy("Team_2").agg(sum("T1_Score").alias("T_Score")) \
        .withColumnRenamed("Team_2", "Team")

    # Get total lost sets for every team
    total_teams_lost_sets = team_1_lost_sets.union(team_2_lost_sets).groupBy("Team").agg(sum("T_Score") \
                                                                                         .alias("Sets_Lost"))
    return total_teams_lost_sets


def get_total_win_points(df):
    # 3)a)
    # Get Home Teams Total Points for each team
    team_1_points = df.groupBy("Team_1").agg(sum("T1_Sum").alias("T_Sum")) \
        .withColumnRenamed("Team_1", "Team")

    # Get Away Teams Total Points for each team
    team_2_points = df.groupBy("Team_2").agg(sum("T2_Sum").alias("T_Sum")) \
        .withColumnRenamed("Team_2", "Team")

    # Get Total Points for each Team
    total_points = team_1_points.union(team_2_points).groupBy("Team").agg(sum("T_Sum").alias("Points_Won"))
    return total_points


def get_total_lost_points(df):
    # 3)b)
    # Get Home Teams Total Lost Points
    team_1_lost_points = df.groupBy("Team_1").agg(sum("T2_Sum").alias("T_Sum")) \
        .withColumnRenamed("Team_1", "Team")

    # Get Away Teams Total Lost Points
    team_2_lost_points = df.groupBy("Team_2").agg(sum("T1_Sum").alias("T_Sum")) \
        .withColumnRenamed("Team_2", "Team")

    # Get Total Lost Points for each Team
    total_lost_points = team_1_lost_points.union(team_2_lost_points).groupBy("Team").agg(
        sum("T_Sum").alias("Points_Lost"))
    return total_lost_points


def create_statistics_dataframe(df):
    # Create team_statistics DataFrame by joining other DataFrames
    team_statistics = get_wins(df)
    team_statistics = team_statistics.join(get_number_of_home_games(df), on="Team", how="left_outer") \
        .join(get_number_of_away_games(df), on="Team", how="left_outer") \
        .join(get_total_number_of_games(get_number_of_home_games(df), get_number_of_away_games(df))
              , on="Team", how="left_outer") \
        .join(get_win_sets(df), on="Team", how="left_outer") \
        .join(get_lost_sets(df), on="Team", how="left_outer") \
        .join(get_total_win_points(df), on="Team", how="left_outer") \
        .join(get_total_lost_points(df), on="Team", how="left_outer")

    # Sort the DataFrame by the "Game_Wins" column in descending order
    team_statistics = team_statistics.orderBy(col("Game_Wins").desc())

    return team_statistics



if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("VolleyBall_Data_Statistics_Table_Spark") \
        .config("spark.sql.debug.maxToStringFields", 100).master('local[*]').getOrCreate()

    # Read a CSV file into a DataFrame
    df = spark.read.csv('file:///home/bigdata/PycharmProjects/sparkVolleyball/data-volleyball.csv',
                        header=True, inferSchema=True)

    df_statistics = create_statistics_dataframe(df)

    # Create a CSV File with team_statistics DataFrame
    df_statistics.write.format('csv').option("header", True).mode('overwrite') \
        .save("file:///home/bigdata/PycharmProjects/sparkVolleyball/team_statistics")

    # Show DataFrame
    df_statistics.show(truncate=False)

    # Stop the Spark session
    spark.stop()
