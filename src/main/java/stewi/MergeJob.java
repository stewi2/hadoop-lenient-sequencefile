package stewi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.Logger;

import stewi.mapred.LenientSequenceFileInputFormat;

public class MergeJob extends Configured implements Tool {

    public static class MergeReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

        Logger logger = Logger.getLogger(MergeReducer.class);
        Configuration conf;

        @Override
        public void configure(JobConf job) {
            conf = job;
        }

        @Override
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output,
                Reporter reporter) throws IOException {

            DynamicBloomFilter seen_rows = createBloomFilter();
            DynamicBloomFilter duplicated_rows = createBloomFilter();

            while(values.hasNext()) {
                Text line = values.next();
                Key filterkey = new Key(line.copyBytes());
                if(!seen_rows.membershipTest(filterkey)) {
                    output.collect(key,line);
                    seen_rows.add(filterkey);
                } else {
                    if(!duplicated_rows.membershipTest(filterkey)) {
                        System.out.printf("Found duplicated row: %d\t%s\n",
                                key.get(), line.toString());
                        reporter.incrCounter("File Merge", "Duplicate Rows", 1);
                        duplicated_rows.add(filterkey);
                    }
//                    System.out.printf("Dropping duplicate row in: %d\t%s\n",
//                            key.get(), line.toString());
                    reporter.incrCounter("File Merge","Duplicates Dropped", 1);
                }
            }
        }

        protected DynamicBloomFilter createBloomFilter() {
            int n_keys = conf.getInt("mergejob.bloom.size", 10000);
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
            return filter;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 1) {
            System.err.println("Usage : File Merge <input path>");
            System.exit(-1);
        }

        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        // Create a JobConf using the processed conf
        JobConf job = new JobConf(conf, MergeJob.class);

        // Process custom command-line options
        Path in = new Path(args[0]);
        Path tmpout = new Path(in, ".merge_tmp");

        FileSystem fs = in.getFileSystem(conf);

        if(fs.exists(tmpout)) {
            System.out.println("Deleting old output directory");
            fs.delete(tmpout,false);
        }

        if(fs.exists(tmpout)) {
            System.out.println("Deleting old tmp directory");
            fs.delete(tmpout,false);
        }

        // Specify various job-specific parameters
        job.setJobName("Merge "+in);
        job.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

        FileOutputFormat.setOutputPath(job, tmpout);

        job.setReducerClass(MergeJob.MergeReducer.class);
        job.setPartitionerClass(TimeOfDayBasedPartitioner.class);

        job.setInputFormat(LenientSequenceFileInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        long size = 0;

        for(FileStatus glob: in.getFileSystem(conf).globStatus(in)) {
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(glob.getPath(), false);
            while(fileStatusListIterator.hasNext()) {
                LocatedFileStatus status = fileStatusListIterator.next();
                size += status.getLen();
                FileInputFormat.addInputPath(job, status.getPath());
            }
        }

        Path[] paths = FileInputFormat.getInputPaths(job);

        long bytes_per_reducer = conf.getInt("mergejob.mb-per-reducer", 128) * 1024 * 1024;
        int n_reducers = (int)Math.ceil((float)size / bytes_per_reducer);
        System.out.printf("Merging %d input files of total size %.2fMB into %d output files.\n",
                paths.length, (float)size/1024/1024, n_reducers);
        job.setNumReduceTasks(n_reducers);

        // Submit the job, then poll for progress until the job is complete
        JobClient jc = new JobClient(job);

        final RunningJob rj = jc.submitJob(job);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if(!rj.getJobStatus().isJobComplete()) {
                        System.out.println("Killing the job because of JVM shutdown");
                        rj.killJob();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        boolean success = jc.monitorAndPrintJob(job, rj);

        if(success) {
            System.out.println("Backing up old Files");
            Path backup = new Path(in, ".merge_backup");
            fs.mkdirs(backup);
            for(Path src: paths) {
                Path dest = new Path(backup,src.getName());
                System.out.println(src + " -> " + dest);
                fs.rename(src, dest);
            }
    
            System.out.println("Moving new files in place");
    
            Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(job,DefaultCodec.class);
            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
    
            for(FileStatus status: fs.listStatus(tmpout)) {
                Path src = status.getPath();
                Path dest = new Path(in, new Path(String.format("merged_help_center.%s.%s%s",
                        rj.getID().toString().substring(4),
                        src.getName().substring(5),
                        codec.getDefaultExtension())));
                System.out.println(src + " -> " + dest);
                fs.rename(src, dest);
            }
        }

        if(fs.exists(tmpout)) {
            System.out.println("Deleting tmp directory");
            fs.delete(tmpout,false);
        }
/*
        for(TaskCompletionEvent event: rj.getTaskCompletionEvents(0)) {
            URL url = new URL(event.getTaskTrackerHttp() +
                    "/tasklog?attemptid=" + event.getTaskAttemptId() +
                    "&plaintext=true" +
                    "&filter=" + TaskLog.LogName.SYSLOG);

            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openConnection().getInputStream()));
            String line = null;
            try {
                while((line = reader.readLine())!=null) {
                    System.out.printf("%s: %s\n", event.getTaskAttemptId(), line);
                }
            } catch(IOException e) {
                System.out.println(e.getMessage());
            } finally {
                reader.close();
            }
        }
*/
        return success ? 0 : 1;
      }

      public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new MergeJob(), args);
        System.exit(res);
      }
}
