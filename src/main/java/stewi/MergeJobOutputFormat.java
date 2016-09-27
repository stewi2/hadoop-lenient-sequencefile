package stewi;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.FilterOutputFormat;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class MergeJobOutputFormat<K, V> extends LazyOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
            throws IOException {
        if(name.startsWith("part")) {
            Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(job,DefaultCodec.class);
            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
            name = "merged_help_center_" + name.substring(4) + codec.getDefaultExtension();
        }
        return super.getRecordWriter(ignored, job, name, progress);
    }

}
