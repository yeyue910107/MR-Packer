package test;
import java.io.IOException;
import java.util.*;
import java.text.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.*;


public class testquery2 extends Configured implements Tool {

	public static class Map extends Mapper<Object, Text, IntWritable, Text> {

		private String filename;
		private int filetag = -1;
		public void setup(Context context) throws IOException, InterruptedException {
			int last_index = -1;
			String path = ((FileSplit)context.getInputSplit()).getPath().toString();
			last_index = path.lastIndexOf('/');
			filename = path.substring(last_index + 1);
			if (filename.compareTo("LINEITEM") == 0) {
				filetag = 2;
			}
			if (filename.compareTo("PART") == 0) {
				filetag = 1;
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] line_buf= line.split("\\|");
			BitSet dispatch = new BitSet(32);
			if (filetag == 1) {
				if (line_buf[3].compareTo("Brand#34") == 0 && line_buf[6].compareTo("MED PACK") == 0) {
					if (!(line_buf[3].compareTo("Brand#34") == 0 && line_buf[6].compareTo("MED PACK") == 0))
						dispatch.set(5);
					if (dispatch.isEmpty())
						context.write(new IntWritable(Integer.parseInt(line_buf[0])), new Text(1+"||"+Integer.parseInt(line_buf[0])+ "|" ));
					else
						context.write(new IntWritable(Integer.parseInt(line_buf[0])), new Text(1+"|"+dispatch.toString()+"|"+Integer.parseInt(line_buf[0])+ "|" ));
				}
			}
			if (filetag == 2) {
				context.write(new IntWritable(Integer.parseInt(line_buf[1])), new Text(2+"||"+Integer.parseInt(line_buf[1])+ "|" +Double.parseDouble(line_buf[4])+ "|" +Double.parseDouble(line_buf[5])+ "|" ));
			}
		}

	}

	public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> v, Context context) throws IOException, InterruptedException {
			Iterator values = v.iterator();
			ArrayList[] tmp_output = new ArrayList[3];
			for (int i = 0; i < 3; i++) {
				tmp_output[i] = new ArrayList();
			}
			String tmp = "";
			Double[] result_0 = new Double[1];
			ArrayList[] d_count_buf_0 = new ArrayList[1];
			int al_line_0 = 0;
			for (int i = 0; i < 1; i++) {
				result_0[i] = 0.0;
				d_count_buf_0[i] = new ArrayList();
			}

			ArrayList al_left_1 = new ArrayList();
			ArrayList al_right_1 = new ArrayList();
			ArrayList al_left_2 = new ArrayList();
			ArrayList al_right_2 = new ArrayList();
			while (values.hasNext()) {
				String line = values.next().toString();
				String dispatch = line.split("\\|")[1];
				tmp = line.substring(2+dispatch.length()+1);
				String[] line_buf = tmp.split("\\|");
				if (line.charAt(0) == '2' && (dispatch.length() == 0 || dispatch.indexOf('0') == -1)) {
					result_0[0] += Double.parseDouble(line_buf[1]);
					al_line_0++;
				}
				if (line.charAt(0) == '2' && (dispatch.length() == 0 || dispatch.indexOf("1") == -1))
					al_left_1.add(tmp);
				if (line.charAt(0) == '1' && (dispatch.length() == 0 || dispatch.indexOf("1") == -1))
					al_right_1.add(tmp);
			}
			String[] line_buf = tmp.split("\\|");
			result_0[0] = result_0[0] / al_line_0;
			tmp_output[0].add(Integer.parseInt(line_buf[0]) + "|"+(0.2 * (result_0[0])) + "|");
			for (int i = 0; i < al_left_1.size(); i++) {
				String[] left_buf_1 = ((String) al_left_1.get(i)).split("\\|");
				for (int j = 0; j < al_right_1.size(); j++) {
					String[] right_buf_1 = ((String) al_right_1.get(j)).split("\\|");
					tmp_output[1].add(Integer.parseInt(left_buf_1[0])+ "|" +Double.parseDouble(left_buf_1[1])+ "|" +Double.parseDouble(left_buf_1[2])+ "|" );
				}
			}
			for (int i = 0; i < tmp_output[1].size(); i++) {
				String[] left_buf_2 = ((String) tmp_output[1].get(i)).split("\\|");
				for (int j = 0; j < tmp_output[0].size(); j++) {
					String[] right_buf_2 = ((String) tmp_output[0].get(j)).split("\\|");
					if (Double.parseDouble(left_buf_2[1]) < Double.parseDouble(right_buf_2[1])) {
						tmp_output[2].add(1+ "|" +Double.parseDouble(left_buf_2[2])+ "|" );
					}
				}
			}
			NullWritable key_op = NullWritable.get();
			for (int i = 0; i < tmp_output[2].size(); i++) {
				String result = (String)tmp_output[2].get(i);
				context.write(key_op, new Text(result));
			}
		}

	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "testquery2");
		job.setJarByClass(testquery2.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new testquery2(), args);
		System.exit(res);
	}

}

