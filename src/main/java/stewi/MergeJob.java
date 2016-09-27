package stewi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import stewi.mapred.LenientSequenceFileInputFormat;

public class MergeJob extends Configured implements Tool {

    public static class MergeMapper extends MapReduceBase implements Mapper<LongWritable, Text, MD5Hash, PairWritable> {
        @Override
        public void map(LongWritable key, Text value, OutputCollector<MD5Hash, PairWritable> output, Reporter reporter)
                throws IOException {
            output.collect(
                    new MD5Hash().digest(key.toString()).digest(value.toString()),
                    new PairWritable(key,value));
        }
    }

    public static class MergeReducer extends MapReduceBase implements Reducer<MD5Hash, PairWritable, LongWritable, Text> {
        @Override
        public void reduce(MD5Hash key, Iterator<PairWritable> values, OutputCollector<LongWritable, Text> output,
                Reporter reporter) throws IOException {
            PairWritable pair = values.next();
            output.collect(pair.getKey(), pair.getValue());
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

        FileInputFormat.addInputPath(job, new Path(args[0]) );
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MergeJob.MergeMapper.class);
        job.setReducerClass(MergeJob.MergeReducer.class);

        job.setInputFormat(LenientSequenceFileInputFormat.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(MD5Hash.class);
        job.setMapOutputValueClass(PairWritable.class);
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
