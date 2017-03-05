import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


/**
 * Created by leon on 16-11-23.
 * in map period, hadoop will automatically sort keys. so in reduce period, we just print the key in order
 */
public class Sort extends Configured implements Tool{
    public static class Map extends Mapper<LongWritable, Text, DoubleWritable, Text>{
        public void map(LongWritable key,Text value,Context context)
                throws IOException,InterruptedException{
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line," ");
            String strName = tokenizer.nextToken();
            String strScore = tokenizer.nextToken();

            Text name = new Text(strName);
            double Score = Double.parseDouble(strScore);
            context.write(new DoubleWritable(Score), name);
        }
    }

    public static class Reduce extends Reducer<DoubleWritable,Text,Text,DoubleWritable>{
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for (Text val : values){
                context.write(val, key);
            }
        }
    }
    public int run(String[] args) throws Exception{
        Job job = new Job(getConf());
        job.setJarByClass(Sort.class);
        job.setJobName("Sort");

        job.setOutputKeyClass(DoubleWritable.class);//map output
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);//Combiner deal with the <key,valuesIterator> in one article before reduce
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);//TextInoutFormat: readlines in python
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0:1;
    }
    public static void main(String[] args) throws Exception{
        int ret = ToolRunner.run(new Sort(), args);
        System.exit(ret);
    }
}
