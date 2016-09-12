//package org.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class mp24 {
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
            Tuple2<String, String> retval = new Tuple2<String, String> ("","");
            String[] tk = s.split(",");
            int i = 0;
            //while (tokenizer.hasMoreTokens()) {
            while (i < tk.length) {
                if (i == 0 || i == 2 || i == 3){
                    items.add(tk[i]);
                }
                i = i+1;
            }
            String key = items.get(2);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
              Date date = formatter.parse(items.get(0));
              Calendar calendar = Calendar.getInstance();
              calendar.setTime(date);
              int min = (calendar.get(Calendar.HOUR_OF_DAY))*60 + calendar.get(Calendar.MINUTE); 
              items.remove(0);
              String str = String.join(",", items);
              // key is MOTD and value is impression, click 
              retval = new Tuple2<String, String>  (Integer.toString(min), str);
            } catch (ParseException e) {
              e.printStackTrace();
            }
            return retval;
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
          List<String> temp = new ArrayList<String>();
          temp.add(Integer.toString(sumi));
          temp.add(Integer.toString(sumcli));
          String str = String.join(", ", temp);
          // sum for impression and clicks for every region
          return str;
        }
      };


   private static final PairFunction<Tuple2<String, String>, Double, String> WORDS_MAPPER2 =
      new PairFunction<Tuple2<String, String>, Double, String>() {
        @Override
        public Tuple2<Double, String> call(Tuple2<String, String> s) throws Exception {
            
            String[] tk = (s._2).split(",");
            Double imp = Double.parseDouble(tk[0].trim());
            Double cli = Double.parseDouble(tk[1].trim());
            Double rate;
            if (imp != 0) {
               rate = cli/imp;  
            }
            else{
               // key is impression-click rate and value is MOTD   
               rate = (double)(-1000.0); 
            }   
            return new Tuple2<Double, String>(rate, s._1);    
        }
      };

      
      private static final Function<Tuple2<Double, String>, Boolean> FF =
      new Function<Tuple2<Double, String>, Boolean>() {
        @Override
        public Boolean call(Tuple2<Double, String> s) throws Exception {
            if (s._1 > (double)(0)) {
               return true;
               // key is MOTD
            }
            return false;
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
    JavaPairRDD<String, String> pairs2 = pairs.reduceByKey(WORDS_REDUCER);
    JavaPairRDD<Double, String> pairs3 = (pairs2.mapToPair(WORDS_MAPPER2)).sortByKey(false);
    JavaPairRDD<Double, String> counter = pairs3.filter(FF);
    List<Tuple2<Double, String>> ctr = counter.take(1);

    FileWriter out = null;
    try {
         out = new FileWriter(args[1]+"/"+"output.txt");
         for (Tuple2<Double, String> tup : ctr) {
             out.write(tup._2);
             out.write('\n');
         }
    } finally {
         if (out != null) {
            out.close();
         }
      }

    //counter.saveAsTextFile(args[1]);
    //pairs3.saveAsTextFile(args[1]);
    /*
    filter(new Function<Tuple2<Double, String>, Boolean>() {
                                                          public Boolean call(Tuple2<Double, String> tup) 
                                                          {
                                                            return tup._1 != (Double)(-1000.0);  
                                                          }
                                                        }
                                                        );//.values();
                                                        */
    
  }

}
