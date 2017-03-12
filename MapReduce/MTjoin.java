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
 * Created by leon on 16-11-29.
 */
public class MTjoin extends Configured implements Tool {
    public static int time = 0;

    public static class Map extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            int i = 0;
            if(line.contains("factoryname") == true || line.contains("addressID") == true){
                return;
            }
            while(line.charAt(i) >= '9' || line.charAt(i) <= '0'){
                i++;
            }
            if(line.charAt(0)>='9' || line.charAt(i) <='0'){
                int j = i-1;
                while(line.charAt(j) != ' ') j--;
                String[] values = {line.substring(0,j),line.substring(i)};
                context.write(new Text(values[1]), new Text("1+" + values[0]));
            }
            else{
                int j = i + 1;
                while(line.charAt(j) != ' ') j++;
                String[] values = {line.substring(0,i+1), line.substring(j)};
                context.write(new Text(values[0]), new Text("2+" + values[1]));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
            if(time==0){
                context.write(new Text("factoryName"), new Text("addressName"));
            }
            int factorynum = 0;
            String factory[] = new String[10];
            int addressnum = 0;
            String address[] = new String[10];
            Iterator ite = values.iterator();
            while(ite.hasNext()){
                String record = ite.next().toString();
                int i = 2;
                char type = record.charAt(0);
                if(type == '1'){
                    factory[factorynum] = record.substring(i);
                    factorynum++;
                }
                else{
                    address[addressnum] = record.substring(i);
                }
            }
            if(factorynum != 0 && addressnum != 0){
                for(int m = 0; m<factorynum; m++){
                    for(int n = 0; n<addressnum; n++){
                        context.write(new Text(factory[m]), new Text(address[n]));
                    }
                }
            }
        }
    }
    public int run(String[] args) throws Exception{
        Job job = new Job(getConf());
        job.setJarByClass(MTjoin.class);
        job.setJobName("mutiple tbale join");

        job.setOutputKeyClass(Text.class);
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
        int ret = ToolRunner.run(new MTjoin(), args);
        System.exit(ret);
    }
}
