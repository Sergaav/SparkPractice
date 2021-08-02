
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;
import java.util.stream.Collectors;


public class SparkRDD {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> awardsPlayers = sc.textFile("/home/serha/sparkHomework/AwardsPlayers.csv")
                .filter(f -> !f.contains("playerID"));
        JavaRDD<String> scoringPlayers = sc.textFile("/home/serha/sparkHomework/Scoring.csv")
                .filter(f -> !f.contains("playerID"));
        JavaRDD<String> masterTable = sc.textFile("/home/serha/sparkHomework/Master.csv");
        JavaRDD<String> teamsTable = sc.textFile("/home/serha/sparkHomework/Teams.csv");


        JavaPairRDD<String, Integer> mapAwards = awardsPlayers.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .mapToPair(x -> new Tuple2<>(x.split(",")[0], 1)).reduceByKey((a, b) -> b + 1);

        JavaPairRDD<String, Integer> goalsCount = scoringPlayers
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .map(f -> f.split(","))
                .filter(f -> f.length > 7 && !f[7].equals(""))
                .mapToPair(line -> new Tuple2<>(line[0], Integer.parseInt(line[7])))
                .reduceByKey(Integer::sum)
                .sortByKey();

        JavaPairRDD<String, Tuple2<Integer, Integer>> joinsOutput = mapAwards.join(goalsCount);


        JavaPairRDD<String, Tuple2<Integer, String>> joinGoalsMaster = goalsCount
                .join(masterTable.mapToPair(line ->
                        new Tuple2<>(line.split(",")[0], (line.split(",")[3] + line.split(",")[4]))));

        JavaPairRDD<String, Object> playerTeams = scoringPlayers.mapToPair(line2 -> new Tuple2<>(line2.split(",")[3], line2.split(",")[0]))
                .join(teamsTable.mapToPair(line3 -> new Tuple2<>(line3.split(",")[2], line3.split(",")[18])))
                .mapToPair(l -> new Tuple2<>(l._2._1, l._2._2))
                .groupByKey().mapToPair(l -> new Tuple2<>(String.valueOf(l._1), String.valueOf(IteratorUtils.toList(l._2.iterator()).stream().distinct().collect(Collectors.toList()))));

        JavaPairRDD<String, Tuple2<Tuple2<Integer, Integer>, Object>> fullData = joinsOutput.join(playerTeams);

        fullData.saveAsTextFile("/home/serha/sparkHomework/outRDD");

    }

}
