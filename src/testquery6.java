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
			if (filename.compareTo("PART") == 0){
				filetag = 1;
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

			String line = value.toString();
			String[] line_buf= line.split("\\|");
			BitSet dispatch = new BitSet(32);
			if (filetag == 1){

				if (line_buf[3].compareTo("BRAND#34") == 0 && line_buf[6].compareTo("MED PACK") == 0){

						context.write(new Text(""), new Text(1+"|"+dispatch.toString()+"|"+Integer.parseInt(line_buf[0])));
				}
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
		Job job = new Job(conf, "testquery6");
		job.setJarByClass(testquery6.class);
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

		int res = ToolRunner.run(new Configuration(), new testquery6(), args);
		System.exit(res);
	}

