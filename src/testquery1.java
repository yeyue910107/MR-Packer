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


	public static class Map extends  Mapper<Object, Text, Text, Text>{

		private String filename;
		private int filetag = -1;
		public void setup(Context context) throws IOException, InterruptedException {

			int last_index = -1, start_index = -1;
			String path = ((FileSplit)context.getInputSplit()).getPath().toString();
			last_index = path.lastIndexOf('/');
			last_index = last_index - 1;
			start_index = path.lastIndexOf('/', last_index);
			filename = path.substring(start_index + 1, last_index + 1);
			if (filename.compareTo("testquery2") == 0){
				filetag = 1;
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

			String line = value.toString();
			String[] line_buf= line.split("\\|");
			BitSet dispatch = new BitSet(32);
			if (filetag == 1){

				context.write(new Text(Integer.parseInt(line_buf[0])+line_buf[1]+Double.parseDouble(line_buf[2])+line_buf[3]+line_buf[4]+line_buf[5]+line_buf[6]), new Text(1+"||"+Integer.parseInt(line_buf[0])+line_buf[1]+Double.parseDouble(line_buf[2])+line_buf[3]+line_buf[4]+line_buf[5]+line_buf[6]+Double.parseDouble(line_buf[7])+Double.parseDouble(line_buf[8])));
			}

		}

	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {

		public void reduce(Text key, Iterable<Text> v, Context context) throws IOExceptiuon, InterruptedException {

			Iterator values = v.iterator();
			ArrayList[] tmp_output = new ArrayList[1];
			for (int i = 0; i < 1; i++) {

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

			while (values.hasNext()) {

				String line = values.next().toString();
				String dispatch = line.split("\\|")[1];
				tmp = line.substring(2+dispatch.length()+1);
				String[] line_buf = tmp.split("\\|");
				if (line.charAt(0) == '1' && (dispatch.length() == 0 || dispatch.indexOf('0') == -1)){

					result_0[0] += (Double.parseDouble(line_buf[7]) * (1 - Double.parseDouble(line_buf[8])));
					al_line_0++;
				}
			}
			String[] line_buf = tmp.split("\\|");
			NullWritable key_op = NullWritable.get();
			for (int i = 0; i < tmp_output[0].size(); i++) {
				String result = (String)tmp_output[0].get(i);
				context.write(key_op, new Text(result));
			}
		}

	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "testquery1");
		job.setJarByClass(testquery1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
setMapperClass(Map.class);
		job.setReduceClass(Reduce.class);
		FileOutputFormat.addInputPath(job, new Path(args[0]));
		FileOutput.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new testquery1(), args);
		System.exit(res);
	}

