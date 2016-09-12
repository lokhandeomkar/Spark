//package org.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.*;
import java.io.*;
//import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class mp22 {
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

  private static final PairFlatMapFunction<String, String, String> WORDS_MAPPER =
      new PairFlatMapFunction<String, String, String>() {
        @Override
        public Iterable<Tuple2<String, String>> call(String s) throws Exception {
         String key;
         List<Tuple2<String, String>> retval = new ArrayList<Tuple2<String, String>>(); 
         List<String> items = new ArrayList<String>();
         String[] tk = s.split(",",-1);
         for(int i = 0; i < tk.length; i++)
         {
             if(tk[i].isEmpty())
             {
                 tk[i] = "null";
             }
         }
         int i = 0;
         while (i < tk.length) {
             i = i+1;
             if (i == 1 || i == 2 || i == 4){
                 items.add(tk[i-1]);
             }
         }
         // now items is ymdh, uid, click
         if (Long.parseLong(items.get(2).trim())==1 && items.get(1) != "null"){
          SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          try {
            Date date = formatter.parse(items.get(0));
            //Calendar calendar = Calendar.getInstance();
            //calendar.setTime(date);
            long mseconds = date.getTime(); //calendar.get(Calendar.SECOND);
            key = Long.toString(mseconds);
            retval.add(new Tuple2<String, String> (key, items.get(0)+","+items.get(1)));
            key = Long.toString(mseconds-1000);
            retval.add(new Tuple2<String, String> (key, items.get(0)+","+items.get(1)));
          } catch (ParseException e) {
            e.printStackTrace();
          }
         }
         return retval;
        }
      };

      private static final Function<Iterable<String>, Iterable<String>> WORDS_MAPPER2 =
      new Function<Iterable<String>, Iterable<String>>() {
        @Override
        public Iterable<String> call(Iterable<String> values) throws Exception {
          List<String> retval = new ArrayList<String>();
          HashMap<Long, String> mp = new HashMap<Long, String>();
          for (String val : values) {
            //context.write(val,val);
              String[] strng = val.split(","); // ymdh and uid
              mp.put(Long.parseLong(strng[1].trim()), strng[0]); // k,v = uid,ymdh 
          }
          Set<Long> keys = mp.keySet();
          Object[] karr = keys.toArray();
          Arrays.sort(karr);
          for (int i = 0; i < karr.length-1; i++) {
              for (int j = i+1; j < karr.length; j++) { 
                  retval.add((mp.get(karr[i]) + ", " + (karr[i]).toString() + ", " + (karr[j]).toString()));
              }
          }
          return retval;
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
    // reads every line from file and generates the (k,v) pairs (msecond, "ymdh,uid")  
    JavaPairRDD<String, String> pairs = file.flatMapToPair(WORDS_MAPPER);
    // groups the values for every key
    JavaPairRDD<String, Iterable<String>> pairsarr = pairs.groupByKey();
    // does the pairing, keys are not useful after the groupByKey() is done 
    JavaPairRDD<String, String> pairs2 = (pairsarr.flatMapValues(WORDS_MAPPER2)).distinct();
    JavaRDD<String> counter = ((pairs2.sortByKey()).values()).distinct();
    //List<String> ctr = counter.take(10);

    FileWriter out = null;

        
    // this is actually the folder
    counter.saveAsTextFile(args[1]);

  }
}
