package org.bigdata.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Administrator on 2017/6/28.
 */
public class LoadData extends Configured implements Tool {


    public static class LoadDataMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        private Text mapOutputValue = new Text();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splited = line.split("\t");
            String formatDate = sdf.format(new Date(Long.parseLong(splited[0].trim())));
            String rowKeyString = splited[1] + ":" + formatDate;

            mapOutputValue.set(rowKeyString + "\t" + line);
            context.write(key, mapOutputValue);

        }
    }

    public static class LoadDataReducer extends TableReducer<LongWritable, Text, NullWritable> {
        public static final String COLUMN_FAMILY = "info";

        @Override
        //Reducer<LongWritable,Text,NullWritable,Mutation>
        protected void reduce(LongWritable key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] splited = value.toString().split("\t");
                String rowKey = splited[0];

                Put put = new Put(rowKey.getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "row".getBytes(), value.getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "reportTime".getBytes(), splited[1].getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "apmac".getBytes(), splited[2].getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "acmac".getBytes(), splited[3].getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "host".getBytes(), splited[4].getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "siteType".getBytes(), splited[5].getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "upPackNum".getBytes(), splited[6].getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "downPackNum".getBytes(), splited[7].getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "upPayLoad".getBytes(), splited[8].getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "downPayLoad".getBytes(), splited[9].getBytes());
                put.add(COLUMN_FAMILY.getBytes(), "httpStatus".getBytes(), splited[9].getBytes());
                context.write(NullWritable.get(), put);
            }
        }
    }

    public static void createTable(String tableName) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "apache.bigdata.com");

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hcd = new HColumnDescriptor("info");
        htd.addFamily(hcd);

        HBaseAdmin admin = new HBaseAdmin(conf);

        if (admin.tableExists(tableName)) {
            System.out.println(tableName + "is exists,trying to recreate the table");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        System.out.println("create new table" + tableName);
        admin.createTable(htd);

    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "apache.bigdata.com");
        conf.set(TableOutputFormat.OUTPUT_TABLE, "phonelog");
        createTable("phonelog");
        Job job = Job.getInstance(
                conf,
                this.getClass().getSimpleName()
        );
        TableMapReduceUtil.addDependencyJars(job);
        job.setJarByClass(getClass());

        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);

        job.setMapperClass(LoadDataMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(LoadDataReducer.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new String[]{
                "hdfs://apache.bigdata.com:8020/input/hbase"
        };

        int intStatus = ToolRunner.run(
                conf,
                new LoadData(),
                args
        );

        System.exit(intStatus);
    }
}
