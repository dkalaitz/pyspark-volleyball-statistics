from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, upper

if __name__ == "__main__":

    # Create a Spark session
    spark = SparkSession.builder.appName("VolleyBall_Data_Processing_Spark") \
            .config("spark.sql.debug.maxToStringFields", 100).master('local[*]').getOrCreate()

    # Read a CSV file into a DataFrame
    df = spark.read.csv('file:///home/bigdata/PycharmProjects/sparkVolleyball/data-volleyball.csv',
                        header=True, inferSchema=True)

    # 1) Remove '%' symbol and show the dataframe
    percentage_cols = ['T1_Srv_Eff', 'T1_Rec_Pos', 'T1_Rec_Perf', 'T1_Att_Kill_Perc',
                       'T1_Att_Eff', 'T1_Att_Sum', 'T2_Srv_Eff', 'T2_Rec_Pos', 'T2_Rec_Perf', 'T2_Att_Kill_Perc',
                       'T2_Att_Eff', 'T2_Att_Sum']

    for col_name in percentage_cols:
        df = df.withColumn(col_name, regexp_replace(col(col_name), "%", ""))

    df.show(truncate=False)

    # 2) Make the names of columns Team_1 and Team_2 in upper case
    teams_columns = ["Team_1", "Team_2"]

    for team_column in teams_columns:
        df = df.withColumn(team_column, upper(team_column))

    # 3) Count volleyball matches and assign it into a variable
    number_of_games = df.count()
    # print(number_of_games)

    # 4) Create total sets per game column
    df = df.withColumn("Total_Sets", col("T1_Score") + col("T2_Score"))

    # 5) Calculate and save in a CSV file, the number of games per sets
    games_per_total_sets = df.groupBy("Total_Sets").count()
    games_per_total_sets.write.format('csv').option("header", True).mode('overwrite') \
        .save("file:///home/bigdata/PycharmProjects/sparkVolleyball/volleyball_games_per_total_sets")

    # 6) Calculate and save in a CSV file, the total games of each team

    # Combine the "Team_1" and "Team_2" columns into a single column "Team"
    teams = df.select(col("Team_1").alias("Team")).union(df.select(col("Team_2").alias("Team")))

    # Calculate the number of matches played by each team
    total_games_per_team = teams.groupBy("Team").count().withColumnRenamed("count", "Games_Played")
    total_games_per_team.write.format('csv').option("header", True).mode('overwrite') \
            .save("file:///home/bigdata/PycharmProjects/sparkVolleyball/total_games_per_team")

    # Stop the Spark session
    spark.stop()



