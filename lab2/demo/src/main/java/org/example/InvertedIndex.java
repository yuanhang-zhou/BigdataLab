package org.example; //替换成你自己的项目名

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvertedIndex {
    //Mapper
    public static class InvertedIndexMapper
            extends Mapper<Object, Text, Text, Text>{
        // 值为1的IntWritable对象
        private final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            //获得文件名
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            //转为小写
            String lowValue = value.toString().toLowerCase();
            //去除标点符号
            String targetValue=lowValue.replaceAll("\\p{Punct}", " ");
            //分词器
            StringTokenizer itr = new StringTokenizer(targetValue);
            //key:单词
            Text word = new Text();
            //value:(doc:num)
            Text doc_num = new Text();
            while (itr.hasMoreTokens()) {
                //设置key
                word.set(itr.nextToken());
                //设置文档value
                doc_num.set(fileName + ":" + one);
                context.write(word, doc_num); // 输出键值对
            }
        }
    }
    //Reducer,key为word,相同的word会分到相同的reducer
    public static class InvertedIndexReducer
            extends Reducer<Text,Text,Text,Text> {
        //key代表输入的键,此处为单词名
        //value代表值,此处为文档名及其计数
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            // 存储文档和对应计数的映射
            Map<String, Integer> map = new HashMap<>();
            //单词出现总数
            int sum = 0;
            //单词出现文档数量
            int num_of_doc = 0;

            //处理filename:num
            for (Text val : values) {
                String[] doc_num = val.toString().split(":"); // 拆分文档ID和计数
                String doc = doc_num[0];                       // 文档ID
                Integer num = Integer.parseInt(doc_num[1]);  // 计数
                num=map.getOrDefault(doc, 0)+num;
                map.put(doc, num); // 更新文档计数
                sum+=num; // 更新总计数
            }

            num_of_doc = map.size(); // 计算文档数量
            double average=sum/(double)num_of_doc; // 计算平均值
            //使用StringBuilder类构建结果字符串
            StringBuilder resultBuilder=new StringBuilder();
            //输出格式：词频,[doc:num;]
            //词频保留两位小数
            //创建DecimalFormat对象并设置格式
            DecimalFormat df = new DecimalFormat("#.00");
            String aver=df.format(average);
            resultBuilder.append(aver);
            resultBuilder.append(",");
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                resultBuilder.append(entry.getKey()).append(":").append(entry.getValue()).append(";"); // 添加文档和计数
            }
            Text result = new Text(); // 结果文本对象
            result.set(resultBuilder.toString()); // 设置结果字符串
            //输出键值对

            Text newkey=new Text();
            StringBuilder mykey=new StringBuilder();
            mykey.append(aver).append(":").append(key.toString());
            newkey.set(mykey.toString());
            context.write(newkey, result);
        }
    }
    //Main
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(); // 配置对象
        Job job = Job.getInstance(conf, "inverted index"); // 创建作业对象
        job.setJarByClass(InvertedIndex.class); // 设置主类
        job.setMapperClass(InvertedIndexMapper.class); // 设置Mapper类
        job.setReducerClass(InvertedIndexReducer.class); // 设置Reducer类
        job.setOutputKeyClass(Text.class); // 设置输出键类型
        job.setOutputValueClass(Text.class); // 设置输出值类型
        FileInputFormat.addInputPath(job, new Path(args[0])); // 设置输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 设置输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1); // 提交作业并等待完成
    }
}