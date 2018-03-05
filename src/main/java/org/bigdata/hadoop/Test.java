package org.bigdata.hadoop;

import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017/7/27.
 */
public class Test {
    public static void main(String[] args) {
        String[] strs = new String[]{"allen","1a","2c","book"};

        for (int i = 0; i < strs.length; i++) {
            if(strs[i].matches("[a-z]*")){
                System.out.println("this is:"+strs[i]);
            }else
                System.out.println("may be:"+strs[i]);

        }
    }
}
