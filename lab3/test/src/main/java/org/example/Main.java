package example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

/**
 * a->b and b->a then a-b
 */   
public class Main {
    public final static String OutDegreeStatPath = "temp/OutDegreeStat/";
    public final static String EdgeConvertPath = "temp/EdgeConvert/";
    public final static String GraphTriangleCountPath = "temp/GraphTriangleCount/";
    private final static String childResPath = GraphTriangleCountPath;
    public final static int ReducerNum = 6;
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("mapreduce.reduce.memory.mb", "2048");  //设置reduce container的内存大小
        conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");  //设置reduce任务的JVM参数

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String inputPath = otherArgs[0];
        String inputTransPath = "input_trans_file";
        int num_of_v = DataPreprocessUtil.Discrete2Sequence(conf, inputPath, inputTransPath);
        otherArgs[0] = inputTransPath;
        
        //任务参数列表,第一个参数为转化后的序列类型数据
        String[] jobargs = {otherArgs[0], ""};
        //输出根据度排序的节点
        jobargs[1] = OutDegreeStatPath;
        OutDegreeStat.main(jobargs);
        //无向图转换
        jobargs[1] = EdgeConvertPath;
        UndirectionalEdgeConvert.main(jobargs);
        //输入为转换后的无向图，输出为三角形计数
        jobargs[0] = EdgeConvertPath;
        jobargs[1] = GraphTriangleCountPath;
        Count.main(jobargs);

        String outputPath = otherArgs[1] + "/part-r-00000";
        long triangleSum = 0;
        FileSystem fs = FileSystem.get(conf);
        //对三角形计数
        for (FileStatus fst: fs.listStatus(new Path(childResPath))) {
            if (!fst.getPath().getName().startsWith("_")) {
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(fst.getPath()));
                Text key = new Text();
                LongWritable value = new LongWritable();
                reader.next(key, value);
                triangleSum += value.get();
                reader.close();
            }
        }
        //写入结果
        FSDataOutputStream outputStream = fs.create(new Path(outputPath));
        outputStream.writeChars("TriangleSum = " + triangleSum + "\n");
        outputStream.close();
        System.out.println("TriangleSum = " + triangleSum);
        System.exit(0);
    }
}