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
			if (filename.compareTo("TINNER") == 0){
				filetag = 1;
			}
			if (filename.compareTo("TOUTER") == 0){
				filetag = 2;
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

			String line = value.toString();
			String[] line_buf= line.split("\\|");
			BitSet dispatch = new BitSet(32);
			if (filetag == 1){

				if (){

						context.write(new IntWritable(), new Text(1+"|"+dispatch.toString()+"|"+(Double.parseDouble(line_buf[1]) / 7.0)));
				}
			}

			if (filetag == 2){

				if (){

						context.write(new IntWritable(), new Text(2+"|"+dispatch.toString()+"|"+(Double.parseDouble(line_buf[1]) / 7.0)));
				}
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
