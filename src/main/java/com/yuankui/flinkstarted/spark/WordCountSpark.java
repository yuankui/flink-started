package com.yuankui.flinkstarted.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.StreamSupport;

public class WordCountSpark {
    public static void main(String[] args) {
        SparkContext sc = new SparkContext();

        RDD<Tuple2<Object, Text>> rdd = sc.newAPIHadoopFile("/data/in", FileInputFormat.class, Object.class, Text.class, new Configuration());


        JavaRDD<String> resRdd = rdd.toJavaRDD()
                .flatMap(tuple -> {
                    String str = new String(tuple._2().getBytes(), 0, tuple._2().getLength());
                    String[] words = str.split("\\s");
                    return Arrays.asList(words).iterator();
                })
                .groupBy(word -> word)
                .mapValues(words -> StreamSupport.stream(words.spliterator(), false)
                        .count())
                .map(tuple -> {
                    return tuple._1() + "\t" + tuple._2();
                });

        resRdd.saveAsTextFile("hdfs:///data/in");
    }
}
