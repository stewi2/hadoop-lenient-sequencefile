package stewi;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class MergeJobOutputFormat<K, V> extends LazyOutputFormat<K, V> {

    @SuppressWarnings("unchecked")
    public static void  setOutputFormatClass(JobConf job, 
        Class<? extends OutputFormat> theClass) {
        job.setOutputFormat(MergeJobOutputFormat.class);
        job.setClass("mapreduce.output.lazyoutputformat.outputformat", theClass, OutputFormat.class);
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
            throws IOException {
        String thename = name;
        if(name.startsWith("part")) {
            Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(job,DefaultCodec.class);
            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
            name = "merged_help_center" + name.substring(4) + codec.getDefaultExtension();
        }
        return super.getRecordWriter(ignored, job, name, progress);
    }

}
