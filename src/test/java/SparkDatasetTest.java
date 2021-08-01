import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import java.io.File;
import java.io.IOException;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.sum;
import static org.junit.jupiter.api.Assertions.*;

class SparkDatasetTest {
    SparkSession session;

    @BeforeEach
    void createSparkSession() {
        session = SparkSession.builder().master("local[*]").getOrCreate();
    }

    @Test
    void fileReaderCSV() {
        long countRows = SparkDataset.fileReaderCSV(session, "/home/serha/IdeaProjects/SparkPractice/AwardsTest.csv").count();
        assertEquals(39, countRows);
    }

    @Test
    void fileWriterAvro() throws IOException {
        Dataset<Row> awards = SparkDataset.fileReaderCSV(session, "/home/serha/IdeaProjects/SparkPractice/AwardsTest.csv");
        SparkDataset.fileWriterAvro(awards, "/home/serha/IdeaProjects/SparkPractice/avro", 1);
        Dataset<Row> awardsAvro = session.read().format("avro").load("/home/serha/IdeaProjects/SparkPractice/avro/");
        assertEquals(awardsAvro.collectAsList().size(), awards.collectAsList().size());
        FileUtils.deleteDirectory(new File("/home/serha/IdeaProjects/SparkPractice/avro/"));
    }

    @Test
    void fileWriterParquet() throws IOException {
        Dataset<Row> awards = SparkDataset.fileReaderCSV(session, "/home/serha/IdeaProjects/SparkPractice/AwardsTest.csv");
        SparkDataset.fileWriterParquet(awards, "/home/serha/IdeaProjects/SparkPractice/parquet", 1);
        Dataset<Row> awardsParquet = session.read().format("parquet").load("/home/serha/IdeaProjects/SparkPractice/parquet/");
        assertEquals(awardsParquet.collectAsList().size(), awards.collectAsList().size());
        FileUtils.deleteDirectory(new File("/home/serha/IdeaProjects/SparkPractice/parquet/"));
    }

    @Test
    void dataTransformation() {
        Dataset<Row> awardsTable = SparkDataset.fileReaderCSV(session, "/home/serha/IdeaProjects/SparkPractice/AwardsTest.csv");
        Dataset<Row> scoringTable = SparkDataset.fileReaderCSV(session, "/home/serha/IdeaProjects/SparkPractice/ScoringTest.csv");
        Dataset<Row> masterTable = SparkDataset.fileReaderCSV(session, "/home/serha/IdeaProjects/SparkPractice/MasterTest.csv");
        Dataset<Row> teamsTable = SparkDataset.fileReaderCSV(session, "/home/serha/IdeaProjects/SparkPractice/TeamsTest.csv");
        Dataset<Row> finalSet = SparkDataset.dataTransformation(awardsTable, scoringTable, masterTable, teamsTable);
        Dataset<Row> awardsCount = awardsTable.groupBy("playerID").count().as("Awards");
        Dataset<Row> goalsCount = scoringTable.groupBy("playerID").agg(sum("G").as("Goals"));
        Dataset<Row> countGoals = awardsCount.join(goalsCount, "playerID").sort(desc("Goals"));
        assertEquals(22, awardsCount.collectAsList().size());
        assertEquals(10, goalsCount.collectAsList().size());
        assertEquals(0, countGoals.collectAsList().size());
        assertEquals(0,finalSet.collectAsList().size());
    }

    @AfterEach
    void closeSparkSession() {
        session.stop();
    }
}