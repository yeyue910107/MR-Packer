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
			if (filename.compareTo("LINEITEM") == 0){
				filetag = 1;
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

			String line = value.toString();
			String[] line_buf= line.split("\\|");
			BitSet dispatch = new BitSet(32);
			if (filetag == 1){

				context.write(new Text(""), new Text(1+"||"+Integer.parseInt(line_buf[1])+Double.parseDouble(line_buf[4])));
			}

		}

	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {

		public void reduce(Text key, Iterable<Text> v, Context context) throws IOExceptiuon, InterruptedException {

			Iterator values = v.iterator();
			ArrayList[] tmp_output = new ArrayList[0];
			for (int i = 0; i < 0; i++) {

				tmp_output[i] = new ArrayList();
			}
			String tmp = "";
			while (values.hasNext()) {

				String line = values.next().toString();
				String dispatch = line.split("\\|")[1];
				tmp = line.substring(2+dispatch.length()+1);
				String[] line_buf = tmp.split("\\|");
			}
			String[] line_buf = tmp.split("\\|");
			NullWritable key_op = NullWritable.get();
		}

	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "testquery9");
		job.setJarByClass(testquery9.class);
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

		int res = ToolRunner.run(new Configuration(), new testquery9(), args);
		System.exit(res);
	}

