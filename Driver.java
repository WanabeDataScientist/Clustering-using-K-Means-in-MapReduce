package hadoopKMeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	// static enum MoreIterations {TOTALITERATIONS}
	static enum MoreIterations {
		numberOfIterations
	}

	public static void main(String[] args) throws Exception {

		Job job;
		int iterationCount = 0;
		long terminationValue = 1;

		while (terminationValue > 0) {
			job = new Job(new Configuration(), "kmeans");
			// setting the input files for the job
			FileInputFormat.setInputPaths(job, new Path(args[0]));

			// Setting the driver class
			job.setJarByClass(DriverforKMeans.class);

			// Setting the Mapper class
			job.setMapperClass(MapperforKMeans.class);
			// Setting the Reducer Class
			job.setReducerClass(ReducerforKMeans.class);
			// Setting the output key type and value type for the Mapper
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);

			// Setting the partitioner class
			job.setPartitionerClass(KMeansPartitioner.class);

			job.setInputFormatClass(TextInputFormat.class);
			// job.setOutputFormatClass(TextOutputFormat.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			// Setting the name of input and output file as per the iteration
			String Input, output;
			if (iterationCount == 0) {

				output = args[1];

			} else {
				output = args[1] + "_" + (iterationCount);

			}
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			Path ofile = new Path(output + "/" + "part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(ofile)));

			System.out.println("Reading the input file");
			File fout = new File(args[2]);// ("/home/manish/workspace/Assignment2/Input");
			// Writing the new centroids to the input centroid file so that
			// mapper and reducer can work with new centroids
			if (fout.exists()) {
				FileOutputStream fos = new FileOutputStream(fout);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
						fos));
				String line;
				while ((line = br.readLine()) != null) {
					System.out.println("Writing to the file");
					bw.write(line);
					bw.newLine();
				}
				bw.close();
			}

			// fetching the value of the counter and assigning it to the
			// terminationValue
			// to check if we have further iterate our mapreduce program
			Counters jobCntrs = job.getCounters();
			terminationValue = jobCntrs.findCounter(
					MoreIterations.numberOfIterations).getValue();
			iterationCount++;

			System.out.println("The no. of iterations:" + iterationCount);

		}
	}
}
