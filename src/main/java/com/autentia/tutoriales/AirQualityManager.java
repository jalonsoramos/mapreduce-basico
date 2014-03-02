package com.autentia.tutoriales;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AirQualityManager extends Configured implements Tool {

	public static class AirQualityMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		private static final String SEPARATOR = ";";

		/**
		 * DIA; CO (mg/m3);NO (ug/m3);NO2 (ug/m3);O3 (ug/m3);PM10 (ug/m3);SH2 (ug/m3);PM25 (ug/m3);PST (ug/m3);SO2 (ug/m3);PROVINCIA;ESTACIÓN <br>
		 * 01/01/1997; 1.2; 12; 33; 63; 56; ; ; ; 19 ;ÁVILA ;Ávila
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			final String[] values = value.toString().split(SEPARATOR);

			// final String date = format(values[0]);
			final String co = format(values[1]);
			// final String no = format(values[2]);
			// final String no2 = format(values[3]);
			// final String o3 = format(values[4]);
			// final String pm10 = format(values[5]);
			// final String sh2 = format(values[6]);
			// final String pm25 = format(values[7]);
			// final String pst = format(values[8]);
			// final String so2 = format(values[9]);
			final String province = format(values[10]);
			// final String station = format(values[11]);

			if (NumberUtils.isNumber(co.toString())) {
				context.write(new Text(province), new DoubleWritable(NumberUtils.toDouble(co)));
			}
		}

		private String format(String value) {
			return value.trim();
		}
	}

	public static class AirQualityReducer extends Reducer<Text, DoubleWritable, Text, Text> {

		private final DecimalFormat decimalFormat = new DecimalFormat("#.##");

		public void reduce(Text key, Iterable<DoubleWritable> coValues, Context context) throws IOException, InterruptedException {
			int measures = 0;
			double totalCo = 0.0f;

			for (DoubleWritable coValue : coValues) {
				totalCo += coValue.get();
				measures++;
			}

			if (measures > 0) {
				context.write(key, new Text(decimalFormat.format(totalCo / measures)));
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("AirQualityManager required params: {input file} {output dir}");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = new Job(getConf());
		job.setJarByClass(AirQualityManager.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(AirQualityMapper.class);
		job.setReducerClass(AirQualityReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AirQualityManager(), args);
	}
}
