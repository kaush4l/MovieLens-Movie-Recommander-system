import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ranking extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new ranking(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration(); // Setting configuration for
													// first job

		Job job = Job.getInstance(conf, "nametag");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0] + "/movies.csv")); // reading the individual file
		FileOutputFormat.setOutputPath(job, new Path(args[0] + "mv")); // Output path for job
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		String userIdVal = args[2]; // Sending arguments seperately for each job
		int id = Integer.parseInt(args[2]);

		Configuration confa = new Configuration();

		confa.set("user", userIdVal);
		confa.setInt("userid", id);

		Job job1 = Job.getInstance(getConf(), "rating"); // Initializing the rating job
		job1.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job1, new Path(args[0] + "/ratings.csv"));
		FileOutputFormat.setOutputPath(job1, new Path(args[0] + "1/"));
		job1.setMapperClass(Map1.class);
		job1.setCombinerClass(Reduce1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.waitForCompletion(true);

		Configuration conf1 = new Configuration();

		conf1.set("user", userIdVal);
		conf1.set("add", args[0]);

		// Starting the ranking job

		Job findjob = Job.getInstance(conf1, "ranking");
		findjob.setJarByClass(this.getClass());

		findjob.setOutputKeyClass(Text.class);
		findjob.setOutputValueClass(Text.class);

		findjob.setMapperClass(findUserMap.class);
		findjob.setNumReduceTasks(0);

		FileInputFormat.addInputPath(findjob, new Path(args[0] + "1/"));
		FileOutputFormat.setOutputPath(findjob, new Path(args[0] + "2/"));

		findjob.waitForCompletion(true);

		Configuration conf2 = new Configuration();

		// New job to get intermediate out

		conf2.set("add", args[0]);
		conf2.set("user", userIdVal);
		conf2.setInt("userid", id);

		Job Caljob = Job.getInstance(conf2, "try");
		Caljob.setJarByClass(this.getClass());

		Caljob.setOutputKeyClass(DoubleWritable.class);
		Caljob.setOutputValueClass(Text.class);

		Caljob.setMapperClass(SimilarFindMap.class);
		Caljob.setReducerClass(SimilarFindReduce.class);
		Caljob.setNumReduceTasks(1);

		FileInputFormat.addInputPath(Caljob, new Path(args[0] + "1/"));
		FileOutputFormat.setOutputPath(Caljob, new Path(args[1]));

		Caljob.waitForCompletion(true);

		Configuration conf3 = new Configuration();

		// starting the job 3

		conf3.set("add", args[0]);
		conf3.set("user", userIdVal);
		conf3.setInt("userid", id);

		Job finjob = Job.getInstance(conf3, "try");
		finjob.setJarByClass(this.getClass());

		finjob.setOutputKeyClass(Text.class);
		finjob.setOutputValueClass(Text.class);

		finjob.setMapperClass(FindMap.class);
		finjob.setNumReduceTasks(0); // Setting reducers to 0

		FileInputFormat.addInputPath(finjob, new Path(args[1]));
		FileOutputFormat.setOutputPath(finjob, new Path(args[1] + "2/"));

		return finjob.waitForCompletion(true) ? 0 : 1;

	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		String movieVal = "";
		String mvname = "";
		String mvgene = "";

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			Text mvid = null;
			if (line.trim() != null) {
				String[] movie = line.split("\\,"); // Splitting the input by
													// tab
				if (movie.length < 4) { // Checking for correctly formatted
										// movies
					mvname = movie[1];
					mvgene = movie[2];
					movieVal = mvname;
					mvid = new Text(movie[0]);
					context.write(mvid, new Text(movieVal));
				} else { // Case for special case movie names
					mvname = "";
					mvid = new Text(movie[0]);
					mvgene = movie[movie.length - 1];
					int loop = movie.length - 1;
					for (int i = 1; i < loop; i++) {
						if (i == 1) {
							mvname = mvname + movie[i];
						} else {
							mvname = mvname + "," + movie[i];
						}
					}
					movieVal = mvname;
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String movval = "";
			for (Text movievalu : values) {
				movval = movievalu.toString();
			}
			context.write(word, new Text(movval));
		}
	}

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			String movieid = "";
			String userid = "";
			String rating = "";
			String result = "";
			if (line.trim() != null) {
				String[] movie = line.split(","); // Splitting the csv file
													// based on comma
				userid = movie[0];
				movieid = movie[1];
				rating = movie[2];
				result = movieid + "::" + rating + ";"; // setting the symbol for easy identification
				context.write(new Text(userid), new Text(result));
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String movval = "";
			for (Text movievalu : values) {
				movval = movval + movievalu.toString();
			}
			context.write(word, new Text(movval));
		}
	}

	public static class findUserMap extends
			Mapper<LongWritable, Text, Text, Text> {

		// Mapper to find the single given user

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] userval = line.split("\\t");

			Configuration conf = context.getConfiguration();
			String userF = conf.get("user");

			if (userval[0].equals("") || userval[0] == null) {
			} else if (userval[0].equals(userF)) {
				context.write(new Text(userF), new Text(userval[1]));
			}
		}
	}

	public static class SimilarFindMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		// Map function for finding the similar user for the input user
		private static String query = "";
		private static HashMap<String, String> user1M = new HashMap<String, String>();
		private static String userF = "";
		// storing the user values in a HashMap for easy access
		// Setup job to manually take the 2nd input
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String add = conf.get("add") + "2/";
			FileSystem fs = FileSystem.get(conf);
			userF = conf.get("user");
			String dirPath = add;
			File dir = new File(dirPath);
			File[] files = dir.listFiles();
			if (files.length == 0) {
				System.out.println("The directory is empty");
			} else {
				for (File aFile : files) {
					if (aFile.getName().contains("part") && !aFile.getName().endsWith("crc")) {
						BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(aFile.toString()))));
						String line;
						line = br.readLine();
						while (line != null) {
								query = line.split("\\t")[1];
								String[] user1 = query.split(";");
								for (String movie : user1) {
									String[] values = movie.split("::");
									user1M.put(values[0], values[1]);
							}
							line = br.readLine();
						}
					}
				}
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			Configuration conf = context.getConfiguration();
			String userF = conf.get("user");
			int userid = conf.getInt("userid", 1);
			userF = Integer.toString(userid);
			String[] compare = line.split("\\t");
			String user2 = compare[0];
			String[] movies2 = compare[1].split(";");

			int match = 0; // Match score the matched rating
			int pmatch = 0; // Score for perfectly matched matched

			HashMap<String, String> user2M = new HashMap<String, String>();

			if (userF.equals(user2)) {
			} else {
				for (String movie : movies2) {
					String[] values = movie.split("::");
					user2M.put(values[0], values[1]);
				}
				for (String movieID : user1M.keySet()) { // checking for the key in both keysets
					if (user2M.keySet().contains(movieID)) {
						match++;

						String user1rat = user1M.get(movieID);
						String user2rat = user2M.get(movieID);
						double rat1 = Double.parseDouble(user1rat);
						double rat2 = Double.parseDouble(user2rat);

						if (rat1 >= 3.0 && rat2 >= 3.0) {
							pmatch++;
							if (rat1 == rat2) {
								pmatch++;
							}
						} else if (rat1 < 3.0 && rat2 < 3.0) {
							pmatch++;
							if (rat1 == rat2) {
								pmatch++;
							}
						}
					}
				}

				if (pmatch == 0) {} else {
					double score = -(double) pmatch / match; // Score to calculate similarity of user
					context.write(new DoubleWritable(score), new Text(line)); // Sending score as key to get sorted list in reducer
				}
			}
		}
	}

	public static class SimilarFindReduce extends Reducer<DoubleWritable, Text, Text, Text> {

		private static String query = "";
		private static String userF = "";
		private static HashMap<String, String> user1M = new HashMap<String, String>();

		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String add = conf.get("add") + "2/";
			FileSystem fs = FileSystem.get(conf);
			userF = conf.get("user");
			String dirPath = add;
			File dir = new File(dirPath);
			File[] files = dir.listFiles();
			if (files.length == 0) {
				System.out.println("The directory is empty");
			} else {
				for (File aFile : files) {
					if (aFile.getName().contains("part") && !aFile.getName().endsWith("crc")) {
						BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(aFile.toString()))));
						String line;
						line = br.readLine();
						while (line != null) {
								query = line.split("\\t")[1];
								String[] user1 = query.split(";");
								for (String movie : user1) {
									String[] values = movie.split("::");
									user1M.put(values[0], values[1]);
							}
							line = br.readLine();
						}
					}
				}
			}
		}

		@Override
		public void reduce(DoubleWritable word, Iterable<Text> listM,
				Context context) throws IOException, InterruptedException {
			for (Text lines : listM) {
				String line = lines.toString();
				HashMap<String, String> user2M = new HashMap<String, String>();
				String[] compare = line.toString().split("\\t");
				String[] movies2 = compare[1].split(";");

				for (String movie : movies2) {
					String[] values = movie.split("::");
					user2M.put(values[0], values[1]);
				}

				TreeMap<Double, ArrayList<String>> sortedM = new TreeMap<Double, ArrayList<String>>();
				ArrayList<String> internal = new ArrayList<String>();
				for (String key : user2M.keySet()) {
					if (user1M.keySet().contains(key)) {
					} else {
						internal.add(key);
						String user2rat = user2M.get(key);
						double rat1 = -(Double.parseDouble(user2rat));
						if (rat1 < -3.0) {
							Collections.sort(internal);
							sortedM.put(rat1, internal);
						}
					}
				}
				ArrayList<Double> rank = new ArrayList<Double>();
				rank.add(-5.0);
				rank.add(-4.5); 
//				rank.add(-4.0);
//				rank.add(-3.5);
				for (Double r : rank) {
					if (sortedM.containsKey(r)) {
						ArrayList<String> movid = sortedM.get(r);
						for (String mid : movid) {
							context.write(new Text(mid), new Text("movie"));
						}
					}
				}
			}
		}
	}

	public static class FindMap extends Mapper<LongWritable, Text, Text, Text> {

		@SuppressWarnings("unused")
		private static String query = "";
		private static String userF = "";
		private static HashMap<String, String> user1M = new HashMap<String, String>();

		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String add = conf.get("add") + "2/";
			FileSystem fs = FileSystem.get(conf);
			userF = conf.get("user");
			String dirPath = add;
			File dir = new File(dirPath);
			File[] files = dir.listFiles();
			if (files.length == 0) {
				System.out.println("The directory is empty");
			} else {
				for (File aFile : files) {
					if (aFile.getName().contains("part") && !aFile.getName().endsWith("crc")) {
						BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(aFile.toString()))));
						String line;
						line = br.readLine();
						while (line != null) {
								query = line.split("\\t")[1];
								String[] user1 = query.split(";");
								for (String movie : user1) {
									String[] values = movie.split("::");
									user1M.put(values[0], values[1]);
							}
							line = br.readLine();
						}
					}
				}
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String mid = line.split("\\t")[0];
			if (user1M.keySet().contains(mid)) {
				context.write(new Text(user1M.get(mid)), new Text(mid));
			}

		}
	}
}