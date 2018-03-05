package com.bigdata.java;



import java.io.*;
import java.util.*;

/**
 * Created by Administrator on 2018/1/9.
 */
public class JavaWordCount {
    private static String path_src = "E:\\wc.txt";
    private static String path_rst ="E:\\MapReduce\\output\\result.txt";
    private static BufferedReader br  = null;
    private static BufferedWriter bw =null;
    private static String line_current = null;
    private static String[] words = null;
    private static List<String>  word_list = new ArrayList<String>();

    public static void main(String[] args) throws IOException {
        File  file = new  File(path_src);
        if(!file.exists()){
            System.out.println("file " + file + "is not existed, exit");
            return;
        }
        try{
            br  = new BufferedReader(new FileReader(file.getPath()));
            line_current = br.readLine();
            while (line_current != null){
                words = line_current.split(" ");
                for (String s: words){
                    if (!s.equals("")){
                        word_list.add(s);
                    }
                }
                line_current = br.readLine();
            }
            for (String str : word_list){
                System.out.println(str);
            }
            //HastSet方式实现
            Set<String> hashSet = new HashSet<String>(word_list);
            for (String str : hashSet){
                System.out.println("word: "+str + ", occur times: " + Collections.frequency(word_list,str));
            }
            //HashMap方式实现
            Map<String,Integer> hashMap = new HashMap<String, Integer>();
            for (String str : word_list){
                Integer count = hashMap.get(str);
                hashMap.put(str,(count == null)?1:count +1);
            }
            //排序
            TreeMap<String,Integer> treeMap = new TreeMap<String,Integer>(hashMap);
            printMap(treeMap);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            closeInputStream(br);
            closeOutputStream(bw);
        }
    }
    public  static  void printMap(Map<String,Integer>map) throws IOException {
        bw = new BufferedWriter(new FileWriter(path_rst));
        Set<String> keys = map.keySet();
        //打印和保存方式一
        System.err.println("-------打印方式一------");
        for (String str: keys){
            System.out.println(str +"\t" + map.get(str) );
        }
        //打印和保存方式二
        System.err.println("-------打印方式二------");
        for (Map.Entry<String,Integer> entry : map.entrySet()){
            System.out.println(entry.getKey()+"\t"+ entry.getValue());
            writeResult(entry.getKey()+"\t"+entry.getValue());
        }
    }
    public  static  void writeResult(String line){

            try {
                if (bw != null){
                bw.write(line);
                bw.newLine();
                bw.flush();
                }
            } catch (IOException e) {
                e.printStackTrace();
                closeOutputStream(bw);
            }

    }

    private static void closeOutputStream(BufferedWriter bw) {
        if(bw != null){
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public  static  void  closeInputStream(BufferedReader br){
        if(br != null){
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
