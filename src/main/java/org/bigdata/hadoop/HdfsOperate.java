package org.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.*;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Created by Administrator on 2017/8/28.
 */
public class HdfsOperate {
    FileSystem fs = null;
    @Before
    public  void getConf() throws URISyntaxException, IOException, InterruptedException {
        fs = FileSystem.get(
                new URI("hdfs://com.hadoop05:8020"),
                new Configuration(),
                "hadoop"
        );
    }

   @Test
    public void testDownload() throws IOException {
       FSDataInputStream in = fs.open(new Path("/input/1.log"));

       FileOutputStream out = new FileOutputStream("D:\\1.log");

       IOUtils.copyBytes(
               in,
               System.out,
               4096,
               true
       );
    }

    @Test
    public void testUpload() throws IOException {
        FileInputStream in = new FileInputStream("D:\\1.log");

        FSDataOutputStream out = fs.append(new Path("/input/1.log"));

        IOUtils.copyBytes(
                in,
                out,
                4096,
                true
        );
    }
    @Test
    public void listFile() throws IOException {
        RemoteIterator<LocatedFileStatus> rit = fs.listFiles(new Path("/output"),true);

       while (rit.hasNext()){
           LocatedFileStatus lfs = rit.next();
           System.out.println(lfs.toString());
       }


    }
}
