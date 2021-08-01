import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class SparkDataset {


    public static Dataset<Row> fileReaderCSV(SparkSession session, String path) {
        return session.read().option("infernSchema", "true")
                .option("header", "true").csv(path);
    }

    public static void fileWriterAvro(Dataset<Row> dataset, String path, int repartition) {
        dataset.repartition(repartition).write().format("avro").save(path);
    }

    public static void fileWriterParquet(Dataset<Row> dataset, String path, int repartition) {
        dataset.repartition(repartition).write().format("parquet").save(path);
    }

    public static Dataset<Row> dataTransformation(Dataset<Row> awardsTable, Dataset<Row> scoringTable, Dataset<Row> masterTable, Dataset<Row> teamsTable) {
        Dataset<Row> awardsCount = awardsTable.groupBy("playerID").count().as("Awards");
        Dataset<Row> goalsCount = scoringTable.groupBy("playerID").agg(sum("G").as("Goals"));
        Dataset<Row> countGoals = awardsCount.join(goalsCount, "playerID").sort(desc("Goals"));

        Dataset<Row> playerGoals = countGoals.join(masterTable, "playerId")
                .select(col("playerID"), concat(col("firstName"), lit(" "), col("lastName")).as("FullName")
                        , col("Awards.count").as("Awards"), col("Goals"));

        Dataset<Row> scoringTeams = scoringTable.join(teamsTable, "tmID")
                .groupBy("playerID").agg(collect_set("name").as("Teams"));

        return playerGoals.join(scoringTeams, "playerID").orderBy(desc("goals"));
    }


    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[*]").getOrCreate();
        session.sparkContext().setLogLevel("ERROR");
        Dataset<Row> awardsTable = SparkDataset.fileReaderCSV(session, "/home/serha/sparkHomework/AwardsPlayers.csv");
        Dataset<Row> scoringTable = SparkDataset.fileReaderCSV(session, "/home/serha/sparkHomework/Scoring.csv");
        Dataset<Row> masterTable = SparkDataset.fileReaderCSV(session, "/home/serha/sparkHomework/Master.csv");
        Dataset<Row> teamsTable = SparkDataset.fileReaderCSV(session, "/home/serha/sparkHomework/Teams.csv");

        Dataset<Row> finalSet = SparkDataset.dataTransformation(awardsTable, scoringTable, masterTable, teamsTable);
        finalSet.show();
        SparkDataset.fileWriterParquet(finalSet, "/home/serha/sparkHomework/out", 3);
        SparkDataset.fileWriterAvro(finalSet, "/home/serha/sparkHomework/outAvro", 5);
        session.stop();

    }
}
