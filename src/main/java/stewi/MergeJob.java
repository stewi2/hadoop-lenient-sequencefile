package stewi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import com.google.common.hash.BloomFilter;

import stewi.mapred.LenientSequenceFileInputFormat;

public class MergeJob extends Configured implements Tool {

    public static class MergeReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

        Configuration conf;

        @Override
        public void configure(JobConf job) {
            conf = job;
        }

        @Override
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output,
                Reporter reporter) throws IOException {
            int n_keys = conf.getInt("mergejob.bloom.size", 1024 * 1024);
            // Copied from BloomMapFile.java:
            // vector size should be <code>-kn / (ln(1 - c^(1/k)))</code> bits for
            // single key, where <code> is the number of hash functions,
            // <code>n</code> is the number of keys and <code>c</code> is the desired
            // max. error rate.
            // Our desired error rate is by default 0.005, i.e. 0.5%
            float errorRate = conf.getFloat("mergejob.bloom.error.rate", 0.005f);
            int hash_count = 5;
            int vectorSize = (int)Math.ceil((double)(-hash_count * n_keys) /
                Math.log(1.0 - Math.pow(errorRate, 1.0/hash_count)));

            DynamicBloomFilter filter = new DynamicBloomFilter(
                    vectorSize, hash_count, Hash.getHashType(conf), n_keys);

            while(values.hasNext()) {
                Text line = values.next();
                Key filterkey = new Key(line.copyBytes());
                if(!filter.membershipTest(filterkey)) {
                    output.collect(key,line);
                    filter.add(filterkey);
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 2) {
            System.err.println("Usage : File Merge <input path> <output path>");
            System.exit(-1);
        }

        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        // Create a JobConf using the processed conf
        JobConf job = new JobConf(conf, MergeJob.class);

        // Process custom command-line options
        Path in = new Path(args[0]);
        Path out = new Path(args[0]);

        // Specify various job-specific parameters
        job.setJobName("Merge "+args[0]+" "+args[1]);
        job.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
        job.set("mapreduce.output.basename", "help_center");

        FileInputFormat.addInputPath(job, new Path(args[0]) );
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setReducerClass(MergeJob.MergeReducer.class);

        job.setInputFormat(LenientSequenceFileInputFormat.class);
        MergeJobOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(4);

        // Submit the job, then poll for progress until the job is complete
        JobClient.runJob(job);

        return 0;
      }

      public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new MergeJob(), args);
        System.exit(res);
      }
}
