package TDE_Codigos.x1;

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

public class NumOfTrans {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/TDE/x1.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "numOfTrans");
        
        
        // registro das classes
        j.setJarByClass(NumOfTrans.class);
        j.setMapperClass(MapForNumOfTrans.class);
        j.setReducerClass(ReduceForNumOfTrans.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }

    public static class MapForNumOfTrans extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String entrada = value.toString();
            String[] colunas = entrada.split(";");

            String pais = colunas[0];

            if (pais.equals("Brazil")) {
                con.write(new Text("Brazil"), new IntWritable(1));
            }

        }
    }

    public static class ReduceForNumOfTrans extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
        	int contagem = 0;
        	for (IntWritable iw : values) {
        		contagem += iw.get();
        	}
        	con.write(key, new IntWritable(contagem));
        }
    }
}
