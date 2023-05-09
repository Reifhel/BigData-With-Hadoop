package TDE_Codigos.x5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import TDE_Codigos.x5.Writables.*;

import java.io.IOException;


public class MinMaxAVG {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/TDE/x5.txt");

        // criacao do job e seu nome
        Job j = Job.getInstance(c, "Media");

        // Registrar as classes
        j.setJarByClass(MinMaxAVG.class);
        j.setMapperClass(MapForMin.class);
        j.setReducerClass(ReduceForMin.class);
        //j.setCombinerClass(CombineForMin.class);

        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(KeyWritable.class);
        j.setMapOutputValueClass(Media.class);
        // REDUCE
        j.setOutputKeyClass(KeyWritable.class);
        j.setOutputValueClass(MinMaxAvgWritable.class);

        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // rodar :)
        j.waitForCompletion(false);

    }

    public static class MapForMin extends Mapper<LongWritable, Text, KeyWritable, Media> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // funcao chamada automaticamente por linha do arquivo

            // obtendo a linha
            String linha = value.toString();

            if(!linha.startsWith("country_or_area")){

                // quebrando em colunas
                String colunas[] = linha.split(";");
    
    
                String year = colunas[1];
                String unit_type = colunas[7];

                KeyWritable formatada = new KeyWritable(year, unit_type);

                Double price = Double.parseDouble(colunas[5]);
                int qtd = 1;
                // enviando dados no formato (chave,valor) para o reduce
                con.write(formatada, new Media(price, qtd));
            }
        }
    }

    public static class CombineForMin extends Reducer<KeyWritable, Media, KeyWritable, Media>{
        public void reduce(KeyWritable key, Iterable<Media> values, Context con)
                throws IOException, InterruptedException {

            // somar as temperaturas e as qtds para cada chave
            double somaValues = 0;
            int somaTotal = 0;
            
            for(Media o : values){
                somaValues += o.getPrice();
                somaTotal += o.getCount();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new Media(somaValues, somaTotal));


        }
    }


    public static class ReduceForMin extends Reducer<KeyWritable, Media, KeyWritable, MinMaxAvgWritable>{
        public void reduce(KeyWritable key, Iterable<Media> values, Context con)
                throws IOException, InterruptedException {

            // somar as temperaturas e as qtds para cada chave
            
            double menorValor = Integer.MAX_VALUE;
            double maiorValor = 0;
            double media = 0;
            
            int somaValor = 0;
            double totalValor = 0;
            
            for(Media o : values){
                Double valorAtual = o.getPrice();
                
                somaValor += o.getCount();
                totalValor += o.getPrice();
                
                if (valorAtual < menorValor) {
                	menorValor = valorAtual;
                }
                if (valorAtual > maiorValor) {
                	maiorValor = valorAtual;
                }
                
            }
            media = totalValor / somaValor;
            // passando para o reduce valores pre-somados
            con.write(key, new MinMaxAvgWritable(maiorValor, menorValor, media));

        }
    }

}