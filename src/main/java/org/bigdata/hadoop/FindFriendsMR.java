package org.bigdata.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

/**
 * Created by Administrator on 2017/7/19.
 */
public class FindFriendsMR extends Configured implements Tool {
    public static class FindFriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text mapOutKey = new Text();
        private Text mapOutValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /**分割字符串*/
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());

            /**存放朋友*/
            Set<String> set = new TreeSet<String>();

            /**存放自己*/
            mapOutValue.set(stringTokenizer.nextToken());

            while (stringTokenizer.hasMoreTokens()) {
                set.add(stringTokenizer.nextToken());
            }
            /**接受朋友*/
            String[] friends = new String[set.size()];
            friends = set.toArray(friends);

            for (int i = 0; i < friends.length; i++) {
                for (int j = i + 1; j < friends.length; j++) {
                    /**朋友之间*/
                    String output = friends[i] + friends[j];
                    mapOutKey.set(output);
                    /** key -> 朋友组合 value -> 自己*/
                    context.write(mapOutKey, mapOutValue);
                }

            }
        }
    }

    public static class FindFriendReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /**以朋友组合作为key，自己作为value*/
            String comFriends = "";
            for (Text val : values) {
                if (comFriends == "") {
                    comFriends = val.toString();
                } else {
                    comFriends = comFriends + "-" + val.toString();
                }
            }
            outputValue.set(comFriends);
            context.write(key, outputValue);

        }
    }

    public int run(String[] args) throws Exception {
        /**1.获取hadoop配置  */
        Configuration config = new Configuration();

        /** 2.生成对应的job */
        Job job = Job.getInstance(
                config,
                this.getClass().getSimpleName()
        );
        job.setJarByClass(getClass());

        /** 3.设置job */
        /** input->map->reducer->output*/
        /**3.1 inputPath */
        Path inPath = new Path(args[0]);
        FileInputFormat.setInputPaths(job,inPath);

        /**3.2 map */
        job.setMapperClass(FindFriendMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        /**3.3 Reduce */
        job.setReducerClass(FindFriendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /**3.4 output */
        Path outPath = new Path(args[1]);
        FileSystem  fs = outPath.getFileSystem(config);
        if (fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        /**4 commit job to yarn*/
        Boolean isSuccess = job.waitForCompletion(true);

        return isSuccess ? 0 :1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(
                conf,
                new FindFriendsMR(),
                args
        );
        System.exit(status);

    }
}
