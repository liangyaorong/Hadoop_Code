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
 */
public class Mean extends Configured implements Tool{
    public static class Map extends Mapper<LongWritable, Text, Text,DoubleWritable>{
        public void map(LongWritable key,Text value,Context context)
                throws IOException,InterruptedException{
            String content = value.toString();
            System.out.println(content);

            StringTokenizer tokenizer = new StringTokenizer(content," ");

            String strName = tokenizer.nextToken();
            String strScore = tokenizer.nextToken();

            Text name = new Text(strName);
            double score = Double.parseDouble(strScore);
            context.write(name, new DoubleWritable(score));
        }
    }

    public static class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException{
            double sum = 0;
            double count = 0;
            Iterator<DoubleWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                sum += iterator.next().get();
                count++;//iterator has no size.
            }
            double average = sum/count;
            context.write(key,new DoubleWritable(average));
        }
    }

    public int run(String[] args) throws Exception{
        Job job = new Job(getConf());
        job.setJarByClass(Mean.class);
        job.setJobName("Mean");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

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
        int ret = ToolRunner.run(new Mean(), args);
        System.exit(ret);
    }
}

