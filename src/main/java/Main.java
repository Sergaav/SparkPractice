import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[*]").getOrCreate();
        session.sparkContext().setLogLevel("ERROR");
        Dataset<Row> awardsTable = session.read().option("infernSchema", "true")
                .option("header", "true").csv("/home/serha/sparkHomework/AwardsPlayers.csv");
        Dataset<Row> scoringTable = session.read().option("infernSchema", "true")
                .option("header", "true").csv("/home/serha/sparkHomework/Scoring.csv");
        Dataset<Row> masterTable = session.read().option("infernSchema", "true")
                .option("header", "true").csv("/home/serha/sparkHomework/Master.csv");
        Dataset<Row> teamsTable = session.read().option("infernSchema", "true")
                .option("header", "true").csv("/home/serha/sparkHomework/Teams.csv");

        Dataset<Row> awardsCount = awardsTable.groupBy("playerID").count().as("Awards");
        Dataset<Row> goalsCount = scoringTable.groupBy("playerID").agg(sum("G").as("Goals"));
        Dataset<Row> countGoals = awardsCount.join(goalsCount, "playerID").sort(desc("Goals"));

        Dataset<Row> playerGoals = countGoals.join(masterTable, "playerId")
                .select(col("playerID"), concat(col("firstName"), lit(" "), col("lastName")).as("FullName")
                        , col("Awards.count"), col("Goals"));

        Dataset<Row> scoringTeams = scoringTable.join(teamsTable, "tmID")
                .groupBy("playerID").agg(collect_set("name").as("Teams"));

        Dataset<Row> finalSet = playerGoals.join(scoringTeams, "playerID").orderBy(desc("goals"));
        finalSet.repartition(5).write().format("parquet").save("/home/serha/sparkHomework/out");


    }
}
