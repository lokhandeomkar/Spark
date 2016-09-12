//package org.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.io.*;

public class mp21 {
  /*
  private static final FlatMapFunction<String, String> WORDS_EXTRACTOR =
      new FlatMapFunction<String, String>() {
        @Override
        public String call(String s) throws Exception {
          // sends a line
          return s;
        }
      };

   */   

  private static final PairFunction<String, String, String> WORDS_MAPPER =
      new PairFunction<String, String, String>() {
        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            List<String> items = new ArrayList<String>();
            String[] tk = s.split(",");
            int i = 0;
            //while (tokenizer.hasMoreTokens()) {
            while (i < tk.length) {
                i = i+1;
                if (i >= 3 && i <= 6){
                    items.add(tk[i-1]);
                }
            }
            String key = items.get(3);
            items.remove(3);
            String str = String.join(",", items);
          return new Tuple2<String, String>(key, str);
        }
      };

  private static final Function2<String, String, String> WORDS_REDUCER =
      new Function2<String, String, String>() {
        @Override
        public String call(String a, String b) throws Exception {
          String[] strng1 = a.split(",");
          String[] strng2 = b.split(",");
          int sumi   = Integer.parseInt(strng1[0].trim()) + Integer.parseInt(strng2[0].trim());
          int sumcli = Integer.parseInt(strng1[1].trim()) + Integer.parseInt(strng2[1].trim());
          int sumcon = Integer.parseInt(strng1[2].trim()) + Integer.parseInt(strng2[2].trim());
          List<String> temp = new ArrayList<String>();
          temp.add(Integer.toString(sumi));
          temp.add(Integer.toString(sumcli));
          temp.add(Integer.toString(sumcon));
          String str = String.join(", ", temp);
          str = " " + str;
          return str;
        }
      };

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }

    SparkConf conf = new SparkConf().setAppName("mp21").setMaster("local");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> file = context.textFile(args[0]);
    JavaPairRDD<String, String> pairs = file.mapToPair(WORDS_MAPPER);
    JavaPairRDD<String, String> counter = pairs.reduceByKey(WORDS_REDUCER);
    List<Tuple2<String, String>> ctr = counter.take(1);
    // this is actually the folder
    //counter.saveAsTextFile(args[1]);

    FileWriter out = null;

    try {
         out = new FileWriter(args[1]+"/"+"output.txt");
         for (Tuple2<String, String> tup : ctr) {
             out.write(tup._1);
             out.write(',');
             out.write(tup._2);
             out.write('\n');
         }
    } finally {
         if (out != null) {
            out.close();
         }
    }

  }
}
