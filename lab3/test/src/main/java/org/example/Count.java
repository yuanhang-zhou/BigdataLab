package example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class Count {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("mapreduce.reduce.memory.mb", "2048");  //设置reduce container的内存大小
        conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");  //设置reduce任务的JVM参数
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "count");

        job.setJarByClass(Count.class);
        //设置Map和Reduce类
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setNumReduceTasks(Main.ReducerNum);
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
    }
    //Mapper:map不需要进行操作
    public static class CountMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void map(IntWritable key, IntWritable value, Context context)
                throws IOException, InterruptedException{
            context.write(key, value);
        }
    }
    //Reducer:计算三角形数量
    public static class CountReducer extends Reducer<IntWritable, IntWritable, Text, LongWritable> {

        private final static String graph = Main.EdgeConvertPath;
        //存储顶点编号和索引的映射关系
        private Map<Integer, Integer> vexIndex;
        //存储邻接表
        private ArrayList<ArrayList<Integer>> vec = new ArrayList<ArrayList<Integer>>();
        //存储三角数量
        private long triangleNum = 0;

        @Override
        public void setup(Context context)
                throws IOException, InterruptedException {
            int cnt = 0;
            int lastVertex = -1;
            ArrayList<Integer> outVertices = new ArrayList<Integer>();
            vexIndex = new TreeMap<Integer, Integer>();
            //获取文件系统的接口
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            // 历边文件，构建邻接表
            for (FileStatus fst: fs.listStatus(new Path(graph))) {
                if (!fst.getPath().getName().startsWith("_")) {
                    //读取序列文件
                    SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(fst.getPath()));
                    //设置key和value
                    IntWritable key = new IntWritable();
                    IntWritable value = new IntWritable();
                    while (reader.next(key, value)) {
                        // 如果顶点发生变化，则存储上一个顶点的邻接表，并更新索引映射
                        if (key.get() != lastVertex) {
                            if (cnt != 0) vec.add(outVertices);
                            vexIndex.put(key.get(), cnt);
                            cnt++;
                            outVertices = new ArrayList<Integer>();
                            outVertices.add(value.get());
                        } else {
                            outVertices.add(value.get());
                        }
                        lastVertex = key.get();
                    }
                    reader.close();
                }
            }
            vec.add(outVertices);
        }

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            for (IntWritable val: values)
                if (vexIndex.containsKey(val.get()))
                    triangleNum += intersect(vec.get(vexIndex.get(key.get())), vec.get(vexIndex.get(val.get())));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            context.write(new Text("TriangleNum"), new LongWritable(triangleNum));
        }
        //计算两个顶点的共同邻居数量，输入为两个点的邻接表
        private long intersect(ArrayList<Integer> vex1, ArrayList<Integer> vex2) {
            // 将一个列表转换为集合
            HashSet<Integer> set = new HashSet<>(vex1);
            long num = 0;
            // 遍历另一个列表，查找共同元素数量
            for (Integer element : vex2) {
                if (set.contains(element)) {
                    num++;
                }
            }
            return num;
        }
    }
}