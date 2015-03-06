package hadoopKMeans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperforKMeans extends
		Mapper<LongWritable, Text, LongWritable, Text> {
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

	// }

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Read number of centroids and current centroids from file
		// calculate distance of given point to each Centroid
		// winnerCentroid = centroid with minimarl distance for this point
		Double[][] centroids = setCentroid();
		double distance = 0;
		double mindistance = 999999999.9f;
		int winnercentroid = -1;
		System.out.println("Starting Mapper calculations");
		String line = value.toString();
		System.out.println(line + " i m here");
		// String[] tmp1 = new String[2];
		System.out.println(line + " i m here");
		// String[] tmp2 = new String[dataPoints];
		// String[] var = new String[dataPoints];
		double[] doubloop = new double[dataPoints];
		System.out.println(line + " i m here");
		String[] tmp1 = line.split("\t");
		String[] tmp2 = tmp1[1].split(",");

		int j = 0;
		for (j = 0; j < tmp2.length; j++) {

			// var[j] = tmp2[j];
			doubloop[j] = Double.parseDouble(tmp2[j]);
			System.out.println("data points " + doubloop[j]);
		}

		int i = 0;
		double sum = 0;
		for (i = 0; i < noOfClusters; i++) {
			distance = 0;
			for (int k = 0; k < dataPoints; k++) {
				distance = distance + (doubloop[k] - centroids[i][k])
						* (doubloop[k] - centroids[i][k]);

			}
			distance = Math.sqrt(distance);

			if (distance < mindistance) {
				mindistance = distance;
				winnercentroid = i;
			}
		}

		System.out.println("The key value emitted is : key: " + winnercentroid
				+ " and value is " + value.toString());
		context.write(new LongWritable(winnercentroid), value);

	}
}
