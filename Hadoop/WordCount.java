/**
 * Created by leon on 16-11-17.
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/**
 * MapReduce change different article into many <></>
 */

public class WordCount {

    /**Map focus on dealing with one line(determined by InputFormat)*/
    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable>{
        //<input key type, input value type, output key type, out put value type>
        //i.e. input:<0, hello world>     output:<hello, 1>, <world, 1>
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key,//0
                        Text value,//"hello world goodbye world" i.e. what you want to count
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException{
            String line = value.toString();//change input value into String
            StringTokenizer tokenizer = new StringTokenizer(line," ");//split intput value by " " and put them into tokenizer
            while (tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());//word = tokenizer.nextToken
                // .nextToken() will return the first element in the token list and delete it from the list
                output.collect(word,one);//collect <word,1> into output ; one is a object, not Int
            }
        }
    }

    /**Reduce focus on dealing with one key.*/
    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, //"hello"...i.e. one token elements.one one one!
                           Iterator<IntWritable> values,//a IntWritable Iterator(like list in python). many 1 in the list
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException{
            int sum = 0;
            while (values.hasNext()){
                sum += values.next().get();//count the key times; .next() return the first element(object) in the list; .get() get the number(Int) in the element(object)
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main (String[] args) //args is String[],containing input and output
            throws Exception{
        JobConf conf = new JobConf(WordCount.class);//conf:config
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);//map output
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);//Map.class in this script
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));//set the input path from args
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        JobClient.runJob(conf);
    }
}
