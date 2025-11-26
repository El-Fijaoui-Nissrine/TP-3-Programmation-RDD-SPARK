package org.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class VentesRDD {
    public static void main(String[] args) {
        // Étape 1 : Créer la configuration Spark
        SparkConf conf = new SparkConf().setAppName("VentesRDD").setMaster("local[*]"); // "local[*]" pour exécuter en local sur tous les cœurs
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Étape 2 : Charger le fichier ventes.txt
        JavaRDD<String> lignes = sc.textFile("ventes.txt");
        // Partie 1 : Total des ventes par ville

        // Étape 3 : Transformer chaque ligne en paire (ville, prix)
        JavaPairRDD<String, Integer> ventesParVille = lignes.mapToPair(line -> {
            String[] parts = line.split(" ");
            String ville = parts[1];
            int prix = Integer.parseInt(parts[3]);
            return new Tuple2<>(ville, prix);
        });
        // Étape 4 : Calculer le total des ventes par ville
        JavaPairRDD<String, Integer> totalParVille = ventesParVille.reduceByKey((a, b) -> a + b);

        // Étape 5 : Afficher le résultat
        System.out.println("=== Total des ventes par ville ===");
        totalParVille.collect().forEach(System.out::println);

        // Partie 2 : Total des ventes par ville et par année

        // Étape 6 : Transformer chaque ligne en paire (ville-année, prix)
        JavaPairRDD<String, Integer> ventesParVilleAnnee = lignes.mapToPair(line -> {
            String[] parts = line.split(" ");
            String annee = parts[0].split("-")[0]; // extraire l'année depuis la date
            String ville = parts[1];
            int prix = Integer.parseInt(parts[3]);
            String key = ville + "-" + annee;
            return new Tuple2<>(key, prix);
        });
        // Étape 7 : Calculer le total par ville et année
        JavaPairRDD<String, Integer> totalParVilleAnnee = ventesParVilleAnnee.reduceByKey((a, b) -> a + b);

        // Étape 8 : Afficher le résultat
        System.out.println("=== Total des ventes par ville et par année ===");
        totalParVilleAnnee.collect().forEach(System.out::println);

        // Étape 9 : Fermer le SparkContext
        sc.close();
    }


}