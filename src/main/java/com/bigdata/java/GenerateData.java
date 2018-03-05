package com.bigdata.java;

/**
 * Created by Administrator on 2018/1/13.
 */
public class GenerateData {
    private int cid;


    public static void main(String[] args) {
      geneAreaData();
    }
    public static void geneAreaData(){
        String[] area = new String[]{"东北","华北","华南","华东"};
        String[] pro1 = new String[]{"黑龙江","辽宁","吉林"};
        String[] pro2 = new String[]{"河北","山东","山西","河南","北京","天津"};
        String[] pro3 = new String[]{"湖南","江西","湖北","广西","广东","福建"};
        String[] pro4 = new String[]{"江苏","浙江","上海","台湾"};
        String[] city1 = new String[]{"哈尔冰","齐齐哈尔","牡丹江","佳木斯","伊春","鸡西"};
        String[] city2 = new String[]{"太原","大同","忻州","朔州","阳泉","吕梁","长治","临汾","晋城","运城","晋中"};

        int cid = 0;
        for(int i = 0;i< area.length;i++){
            for (int a =0;a <pro1.length ;a++){
                for (int  b = 0;b<city1.length;b++){
                    StringBuffer sb = new StringBuffer();
                    cid +=1;
                    sb = sb.append(cid).append(",").append(area[i]).append(",").append(pro1[a]).append(",").append(city1[b]);
                    System.out.println(sb.toString());
                }

            }

        }
    }
}
