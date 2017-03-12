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
 * Created by leon on 16-11-26.
 * from childname-parentname get grandchildname-grandparentname
 * map: use the join value as key, and all the info as values
 * reduce: go throw all the info in same key and find out grandchild and grandparent
 */
public class STjoin extends Configured implements Tool {
    public static int time = 0;

    public static class Map extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, " ");
            String childname = tokenizer.nextToken();
            String parentname = tokenizer.nextToken();
            if (childname.compareTo("child") != 0){

                String relationtype = "1";
                context.write(new Text(parentname), new Text(relationtype + "+" + childname + "+" + parentname));  //left table
                relationtype = "2";
                context.write(new Text(childname), new Text(relationtype + "+" + childname + "+" + parentname));  // right table
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
            if(time ==0){
                context.write(new Text("grandchild"), new Text("grandparent"));
               time++;
            }
            int grandchildnum = 0;
            String grandchild[] = new  String[10];//string list
            int grandparentnum = 0;
            String grandparent[] = new String[10];
            Iterator ite = values.iterator();
            while(ite.hasNext()){
                String info = ite.next().toString();
                int len = info.length();
                if(len == 0){
                    continue;
                }
                StringTokenizer tokenizer = new StringTokenizer(info, "+");
                String relationType = tokenizer.nextToken();
                String childname = tokenizer.nextToken();
                String parentname = tokenizer.nextToken();

                if(relationType.equals("1")){
                    grandchild[grandchildnum] = childname;
                    grandchildnum++;
                }
                else {
                    grandparent[grandparentnum] = parentname;
                    grandparentnum++;
                }
            }
            if(grandparentnum != 0 && grandchildnum !=0){
                for(int m = 0; m<grandchildnum; m++){
                    for(int n = 0; n<grandparentnum; n++){
                        context.write(new Text(grandchild[m]), new Text(grandparent[n]));
                    }
                }
            }
        }
    }

    public int run(String[] args) throws Exception{
        Job job = new Job(getConf());
        job.setJarByClass(STjoin.class);
        job.setJobName("single tbale join");

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
        int ret = ToolRunner.run(new STjoin(), args);
        System.exit(ret);
    }
}
