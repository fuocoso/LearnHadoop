package org.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;


public class PartColMR extends Configured implements Tool {
    public static class PartColMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
      //  private NullWritable outKey = null;
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取输入文件路径
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
            if (pathName.contains("customer")) {
                String line = value.toString();
                String[] strs = line.split(",");
                outValue.set("customer"+"\t"+strs[0]+"\t"+strs[1]);
                context.write(NullWritable.get(),outValue);
            } else if (pathName.contains("order")) {
                String line = value.toString();
                String[] strs = line.split(",");
                outValue.set("order"+"\t"+strs[1]);
                context.write(NullWritable.get(), outValue);
            }
        }
    }



    public int run(String[] args) throws Exception {
        //1.获取hadoop的配置信息
        Configuration config = super.getConf();
        //config.set();

        //2.生成对应的job
        Job job = Job.getInstance(
                config,
                this.getClass().getSimpleName()
        );
        job.setJarByClass(getClass());

        //3:设置job内容
        //input  -> map ->reduce -> output
        //3.1 输入
        Path inPath = new Path(args[0]);
        FileInputFormat.setInputPaths(job, inPath);


        //3.2:map
        job.setMapperClass(PartColMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //3.4输出
        Path outPath = new Path(args[1]);
        //输出目录如果存在自动删除

        FileSystem fs = outPath.getFileSystem(config);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileOutputFormat.setOutputPath(job, outPath);


        //4.提交的job运行是否成功
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new String[]{
                "E:\\mapReduce\\join",
                "E:\\mapReduce\\output"
        };

        int status = ToolRunner.run(conf,
                new PartColMR(), args);
        System.exit(status);
    }
}
