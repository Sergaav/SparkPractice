import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

        Dataset<Row> awardsCount = awardsTable.groupBy("playerID").count();
        Dataset<Row> goalsCount = scoringTable.groupBy("playerID").agg(sum("G"));
        awardsCount.join(goalsCount,"playerID").show();

    }
}
