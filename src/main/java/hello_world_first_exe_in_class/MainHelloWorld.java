package hello_world_first_exe_in_class;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/**
 * @author Evgeny Borisov
 */
public class MainHelloWorld {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master");

        SparkConf sparkConf = new SparkConf().setAppName("hello world").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        sc.setLogLevel("WARN");

        JavaRDD<String> rdd = sc.textFile("data/hello.txt");
        rdd.filter(line->line.startsWith("j")).collect().forEach(System.out::println);
    }
}
