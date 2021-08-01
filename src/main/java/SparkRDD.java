import net.minidev.json.JSONUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class SparkRDD {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> awardsPlayers = sc.textFile("/home/serha/sparkHomework/AwardsPlayers.csv");
        JavaRDD<String> scoringPlayers = sc.textFile("/home/serha/sparkHomework/Scoring.csv");
        JavaRDD<String> masterTable = sc.textFile("/home/serha/sparkHomework/Master.csv");
        JavaRDD<String> teamsTable = sc.textFile("/home/serha/sparkHomework/Teams.csv");
        Map<String, Long> mapAwards = awardsPlayers.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .map(x -> x.split(",")[0]).countByValue();

        mapAwards.forEach((x,y)-> System.out.println(x+" "+y));

    }

}
