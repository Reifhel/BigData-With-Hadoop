package basic;

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


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/bible.txt");

        // arquivo de saida
        Path output = new Path("output/contagem.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");
        
        
        // registro das classes
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);

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

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
    	/*
    	 * 4 tipos de map
    	 * 1o tipo: tipo da chave de entrada (long) => offset (irrelevante)
    	 * 2o tipo: tipo do valor de entrada (text) => linha do arquivo
    	 * 3o tipo: tipo da chave de saída (text) => palavra a ser contabilizada
    	 * 4o tipo: tipo do valor de saída (int) => numero x (encontramos a palavra x vezes)
    	 */
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
        	String entrada = value.toString();
        	
        	String[] palavras = entrada.split(" ");
        	
        	for(String s : palavras) {
        		Text chave = new Text(s);
        		IntWritable valor = new IntWritable(1);
        		
        		con.write(chave, valor);
        	}
        }
    }
    
    /*
     * 1o tipo: tipo da chave de entrada (text) 	=> palavra
     * 2o tipo: tipo do valor de entrada (int) 		=> ocorrencia = 1
     * 3o tipo: tipo da chve de entreada (text) 	=> palavra
     * 4o tipo: tipo do valor de entrada (int)		=> contagem
     */

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
        	int contagem = 0;
        	for (IntWritable iw : values) {
        		contagem += iw.get();
        	}
        	con.write(key, new IntWritable(contagem));
        }
    }

}