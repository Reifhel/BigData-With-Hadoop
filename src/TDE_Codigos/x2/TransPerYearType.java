package TDE_Codigos.x2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import basic.Teste.Map;
import basic.Teste.Reduce;

public class TransPerYearType {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/TDE/x2.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "TransPerYearType");
        
        
        // registro das classes
        j.setJarByClass(TransPerYearType.class);
        j.setMapperClass(MapForTransPerYearType.class);
        j.setReducerClass(ReduceForTransPerYearType.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(KeyWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(KeyWritable.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }

    public static class MapForTransPerYearType extends Mapper<LongWritable, Text, KeyWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String entrada = value.toString();

            if(!entrada.startsWith("country_or_area")){

                String[] colunas = entrada.split(";");

                String type = colunas[4];
                String year = colunas[1];

                KeyWritable kw = new KeyWritable();
                kw.setYear(year);
                kw.setType(type);

                con.write(kw, new IntWritable(1));
            }

        }
    }

    public static class ReduceForTransPerYearType extends Reducer<KeyWritable, IntWritable, KeyWritable, IntWritable> {

        public void reduce(KeyWritable key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
        	int contagem = 0;
        	for (IntWritable iw : values) {
        		contagem += iw.get();
        	}
        	con.write(key, new IntWritable(contagem));
        }
    }
    
}
