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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.partition.*;


	public static class Map extends  Mapper<Object, Text, IntWritable, Text>{

		private String filename;
		private int filetag = -1;
		public void setup(Context context) throws IOException, InterruptedException {

			int last_index = -1, start_index = -1;
			String path = ((FileSplit)context.getInputSplit()).getPath().toString();
			last_index = path.lastIndexOf('/');
			last_index = last_index - 1;
			start_index = path.lastIndexOf('/', last_index);
			filename = path.substring(start_index + 1, last_index + 1);
			if (filename.compareTo("testquery3") == 0){
				filetag = 1;
			}
			if (filename.compareTo("NATION") == 0){
				filetag = 2;
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

			String line = value.toString();
			String[] line_buf= line.split("\\|");
			BitSet dispatch = new BitSet(32);
			if (filetag == 1){

				context.write(new IntWritable(Integer.parseInt(line_buf[8])), new Text(1+"||"+Integer.parseInt(line_buf[0])+line_buf[1]+Double.parseDouble(line_buf[2])+line_buf[3]+line_buf[4]+line_buf[5]+Double.parseDouble(line_buf[6])+Double.parseDouble(line_buf[7])+Integer.parseInt(line_buf[8])));
			}

			if (filetag == 2){

				context.write(new IntWritable(Integer.parseInt(line_buf[0])), new Text(2+"||"+line_buf[1]+Integer.parseInt(line_buf[0])));
			}

		}

	}

	public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> v, Context context) throws IOExceptiuon, InterruptedException {

			Iterator values = v.iterator();
			ArrayList[] tmp_output = new ArrayList[1];
			for (int i = 0; i < 1; i++) {

				tmp_output[i] = new ArrayList();
			}
			String tmp = "";
			ArrayList al_left_0 = new ArrayList();
			ArrayList al_right_0 = new ArrayList();
			while (values.hasNext()) {

				String line = values.next().toString();
				String dispatch = line.split("\\|")[1];
				tmp = line.substring(2+dispatch.length()+1);
				String[] line_buf = tmp.split("\\|");
				if (line.charAt(0) == '1' && (dispatch.length() == 0 || dispatch.indexOf("1") == -1))
					al_left_1.add(tmp);
				if (line.charAt(0) == '2' && (dispatch.length() == 0 || dispatch.indexOf("1") == -1))
					al_right_1.add(tmp);
			}
			String[] line_buf = tmp.split("\\|");
			for (int i = 0; i < al_left_0.size(); i++) {
				String[] left_buf_0 = ((String) al_left_0.get(i)).split("\\|");
				for (int j = 0; j < al_right_0.size(); j++) {
					String[] right_buf_0 = ((String) al_right_0.get(j)).split("\\|");
					tmp_output[0].add(Integer.parseInt(left_buf_0[0])+left_buf_0[1]+Double.parseDouble(left_buf_0[2])+left_buf_0[3]+right_buf_0[0]+left_buf_0[4]+left_buf_0[5]+Double.parseDouble(left_buf_0[6])+Double.parseDouble(left_buf_0[7]));
				}
			}
			NullWritable key_op = NullWritable.get();
			for (int i = 0; i < tmp_output[0].size(); i++) {
				String result = (String)tmp_output[0].get(i);
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
setMapperClass(Map.class);
		job.setReduceClass(Reduce.class);
		FileOutputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.addInputPath(job, new Path(args[1]));
		FileOutput.setOutputPath(job, new Path(args[2]));
		return (job.waitForCompletion) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new testquery2(), args);
		System.exit(res);
	}

