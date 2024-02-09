from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import data_team_statistics as volleyball_statistics_calculator
from pyspark.sql.functions import when, col, lit
from pyspark.sql import SparkSession

first_csv_read = False

def update_statistics(df_new_game, df_old_statistics):
    df_new_game = df_new_game.select(col("HomeTeam").alias("Team_1"),
                                     col("AwayTeam").alias("Team_2"),
                                     col("HomeSets").alias("T1_Score"),
                                     col("AwaySets").alias("T2_Score"),
                                     col("HomePoints").alias("T1_Sum"),
                                     col("AwayPoints").alias("T2_Sum"))

    df_new_game = df_new_game.withColumn("Winner",
                                         when(col("T1_Score") == 3, 0)
                                         .when(col("T2_Score") == 3, 1).otherwise(None))
    df_new_game = volleyball_statistics_calculator.create_statistics_dataframe(df_new_game)

    # Rename columns in the new table to avoid conflicts during the join
    df_new_game = df_new_game.select(
        col("Team"),
        col("Game_Wins").alias("Game_Wins_new"),
        col("Home_Games_Played").alias("Home_Games_Played_new"),
        col("Away_Games_Played").alias("Away_Games_Played_new"),
        col("Total_Games_Played").alias("Total_Games_Played_new"),
        col("Sets_Won").alias("Sets_Won_new"),
        col("Sets_Lost").alias("Sets_Lost_new"),
        col("Points_Won").alias("Points_Won_new"),
        col("Points_Lost").alias("Points_Lost_new")
    )
    df_new_statistics = df_old_statistics.join(df_new_game, on="Team", how="left_outer")

    # List of columns to be summed
    columns_to_sum = ["Game_Wins", "Home_Games_Played", "Away_Games_Played",
                      "Total_Games_Played", "Sets_Won", "Sets_Lost",
                      "Points_Won", "Points_Lost"]
    # Loop through the columns and sum them up, handling null values
    for column in columns_to_sum:
        df_new_statistics = df_new_statistics.withColumn(column,
                                                         when(col(f"{column}_new").isNotNull(),
                                                              col(f"{column}_new") + col(f"{column}")).otherwise(
                                                             col(f"{column}"))
                                                         )
    # Drop the new columns
    df_new_statistics = df_new_statistics.drop(*[f"{column}_new" for column in columns_to_sum])

    df_new_statistics = df_new_statistics.orderBy(col("Game_Wins").desc())
    return df_new_statistics

def update_statistics_udf(batch_df, df_old_statistics):

    home_team = batch_df.select("HomeTeam").collect()[0][0]
    away_team = batch_df.select("AwayTeam").collect()[0][0]
    home_sets = batch_df.select("HomeSets").collect()[0][0]
    away_sets = batch_df.select("AwaySets").collect()[0][0]
    home_points = batch_df.select("HomePoints").collect()[0][0]
    away_points = batch_df.select("AwayPoints").collect()[0][0]

    # Create a new DataFrame with the streaming data
    df_new_game = spark.createDataFrame(
        [(home_team, away_team, home_sets, away_sets, home_points, away_points)],
        ["HomeTeam", "AwayTeam", "HomeSets", "AwaySets", "HomePoints", "AwayPoints"]
    )

    df_updated = update_statistics(df_new_game, df_old_statistics)

    # Update the global df_statistics DataFrame
    global df_statistics
    df_statistics = df_updated

    # Show Updated Statistics
    df_statistics.show(truncate=False)

    # Create a CSV File with updated team statistics DataFrame
    df_updated.write.format('csv').option("header", True).mode('overwrite') \
        .save("file:///home/bigdata/PycharmProjects/sparkVolleyball/team_statistics_updated")


if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("VolleyBall_Data_Statistics_Streaming") \
        .config("spark.sql.debug.maxToStringFields", 100).master('local[*]').getOrCreate()

    # Read a CSV file into a DataFrame
    if not first_csv_read:
        df = spark.read.csv('file:///home/bigdata/PycharmProjects/sparkVolleyball/data-volleyball.csv',
                            header=True, inferSchema=True)
        df_statistics = volleyball_statistics_calculator.create_statistics_dataframe(df)
        df_statistics.show(truncate=False)
        first_csv_read = True


    # Define the schema for the streaming data
    userSchema = StructType([
        StructField("HomeTeam", StringType(), False),
        StructField("AwayTeam", StringType(), False),
        StructField("HomeSets", IntegerType(), False),
        StructField("AwaySets", IntegerType(), False),
        StructField("HomePoints", IntegerType(), False),
        StructField("AwayPoints", IntegerType(), False)
    ])
    df_newGame = (spark.readStream.format('csv').option('sep', ',').
                  option('header', 'true').schema(userSchema).option('path','file:///home/bigdata/Λήψεις/').load())


    # Start the streaming query with foreachBatch
    df_updated_streaming = df_newGame \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(lambda batch_df, batch_id: update_statistics_udf(
            batch_df,
            df_statistics
        )) \
        .start()

    # Wait for the streaming to finish
    df_updated_streaming.awaitTermination()

    # Stop the Spark session
    spark.stop()
