package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyseLogs {

    // Regex pour parser une ligne Apache log
    private static final Pattern LOG_PATTERN = Pattern.compile(
            // IP
            "^(\\S+) " +
                    // identifiant
                    "\\S+ \\S+ " +
                    // date
                    "\\[([^]]+)] " +
                    // méthode + URL + protocol
                    "\"(\\S+) (\\S+) (\\S+)\" " +
                    // code HTTP
                    "(\\d{3}) " +
                    // taille
                    "(\\S+)"
    );

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Log Analyzer").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. Lecture fichier
        JavaRDD<String> lines = sc.textFile("src/main/resources/access.log");

        // 2. Extraction des champs
        JavaRDD<LogEntry> parsed = lines.map(AnalyseLogs::parseLog).filter(x -> x != null);

        // 3. Statistiques simples
        long totalRequests = parsed.count();
        long totalErrors = parsed.filter(e -> e.code >= 400).count();
        double errorPercent = (totalErrors * 100.0) / totalRequests;

        System.out.println("=== STATISTIQUES ===");
        System.out.println("Total requêtes : " + totalRequests);
        System.out.println("Total erreurs (>=400) : " + totalErrors);
        System.out.println("Pourcentage erreurs : " + errorPercent + "%");


        // 4. Top 5 IP
        JavaPairRDD<String, Integer> ipCount = parsed
                .mapToPair(e -> new Tuple2<>(e.ip, 1))
                .reduceByKey(Integer::sum)
                .sortByKey(false);

        System.out.println("\n=== TOP 5 IP ===");
        ipCount
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .take(5)
                .forEach(System.out::println);

        // 5. Top 5 ressources
        JavaPairRDD<String, Integer> resourceCount = parsed
                .mapToPair(e -> new Tuple2<>(e.url, 1))
                .reduceByKey(Integer::sum);

        System.out.println("\n=== TOP 5 RESSOURCES ===");
        resourceCount
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .take(5)
                .forEach(System.out::println);

        // 6. Répartition par code HTTP
        JavaPairRDD<Integer, Integer> codeDist = parsed
                .mapToPair(e -> new Tuple2<>(e.code, 1))
                .reduceByKey(Integer::sum);

        System.out.println("\n=== REPARTITION PAR CODE HTTP ===");
        codeDist.collect().forEach(System.out::println);

        sc.stop();
    }

    //-----------------------------------------
    // PARSER D'UNE LIGNE LOG
    //-----------------------------------------
    private static LogEntry parseLog(String line) {
        Matcher m = LOG_PATTERN.matcher(line);
        if (!m.find()) return null;

        String ip = m.group(1);
        String date = m.group(2);
        String method = m.group(3);
        String url = m.group(4);
        int code = Integer.parseInt(m.group(6));
        int size = m.group(7).equals("-") ? 0 : Integer.parseInt(m.group(7));

        return new LogEntry(ip, date, method, url, code, size);
    }

    //-----------------------------------------
    // Classe POJO
    //-----------------------------------------
    public static class LogEntry {
        String ip;
        String date;
        String method;
        String url;
        int code;
        int size;

        public LogEntry(String ip, String date, String method, String url, int code, int size) {
            this.ip = ip;
            this.date = date;
            this.method = method;
            this.url = url;
            this.code = code;
            this.size = size;
        }
    }
}

