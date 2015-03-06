package hadoopKMeans;

import hadoopKMeans.DriverforKMeans.MoreIterations;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerKMeans extends
		Reducer<LongWritable, Text, LongWritable, Text> {
	int noOfClusters;
	int dataPoints;

	public Double[][] setCentroid() throws NumberFormatException, IOException {
		InputStream in = new FileInputStream(new File("Input"));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;

		int j;
		noOfClusters = 0;
		dataPoints = 0;
		Double[][] centroids = new Double[3][10];
		while ((line = reader.readLine()) != null) {
			String[] tokens = line.split("\t");

			String[] coordinate = tokens[1].split(",");
			dataPoints = coordinate.length;
			for (j = 0; j < coordinate.length; j++) {
				centroids[noOfClusters][j] = Double.parseDouble(coordinate[j]);

			}
			noOfClusters++;
		}
		reader.close();
		System.out.println("Centroids returned to the mapper:" + noOfClusters);
		return centroids;
	}

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Hello Reducer");
		// Reading the centroid file
		Double[][] centroids = setCentroid();
		int count = 0, j = 0;
		double[] newCentroid = new double[dataPoints];
		String[] redresult = new String[dataPoints];
		// Reading the values of the datapoints and adding them
		for (Text value : values) {
			count++;
			String[] tmp1 = value.toString().split("\t");

			String[] coordinate = tmp1[1].split(",");
			for (j = 0; j < coordinate.length; j++) {
				newCentroid[j] = newCentroid[j]
						+ Double.parseDouble(coordinate[j]);

			}

		}
		// Calculating the average of each coordinates and storing it in
		// newCentroid
		for (j = 0; j < newCentroid.length; j++) {
			newCentroid[j] = newCentroid[j] / count;
			redresult[j] = String.valueOf(newCentroid[j]);
			// System.out.println("redresult" + j + "  " + redresult[j]);
		}

		// Constructing the output for reducer
		String write = "";
		for (String v : redresult) {
			write = write + v + ",";
		}
		// Calculating the distance between the new centroid and the old
		// centroid
		int keys = Integer.parseInt(key.toString());
		double distance = 0;
		for (int k = 0; k < dataPoints; k++) {
			distance = distance + (centroids[keys][k] - newCentroid[k])
					* (centroids[keys][k] - newCentroid[k]); //

		}
		System.out
				.println("The distance between the new centroid and the old is"
						+ Math.sqrt(distance));

		write = write.substring(0, write.length() - 1);

		Text result = new Text(write);

		System.out.println("The key value emitted by reducer is key: "
				+ key.toString() + "value: " + write);
		// Checking the convergence criteria and if met incrementing the
		// numberOfIterations by -1
		if (Math.sqrt(distance) > 0.01) {
			context.getCounter(MoreIterations.numberOfIterations).increment(1L);
		}
		context.write(key, result);
	}
}
