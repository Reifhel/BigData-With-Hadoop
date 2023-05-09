package TDE_Codigos.x4;

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

import TDE_Codigos.x4.Writables.*;

import java.io.IOException;


public class AveragePrice {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/TDE/x4.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "Media");


        // Registrar as classes
        j.setJarByClass(AveragePrice.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(KeyWritable.class);
        j.setMapOutputValueClass(PriceAvgWritable.class);
        // REDUCE
        j.setOutputKeyClass(KeyWritable.class);
        j.setOutputValueClass(FloatWritable.class);

        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // rodar :)
        j.waitForCompletion(false);

    }

    public static class MapForAverage extends Mapper<LongWritable, Text, KeyWritable, PriceAvgWritable> {
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
                String category = colunas[9];

                KeyWritable formatada = new KeyWritable(year, unit_type, category);

                Double price = Double.parseDouble(colunas[5]);
                int qtd = 1;
    
                // enviando dados no formato (chave,valor) para o reduce
                con.write(formatada,
                        new PriceAvgWritable(price, qtd));
            }
            



        }
    }

    public static class CombineForAverage extends Reducer<KeyWritable, PriceAvgWritable, KeyWritable, PriceAvgWritable>{
        public void reduce(KeyWritable key, Iterable<PriceAvgWritable> values, Context con)
                throws IOException, InterruptedException {

            // somar as temperaturas e as qtds para cada chave
            double somaValues = 0;
            int somaTotal = 0;
            for(PriceAvgWritable o : values){
                somaValues += o.getPrice();
                somaTotal += o.getCount();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new PriceAvgWritable(somaValues, somaTotal));


        }
    }


    public static class ReduceForAverage extends Reducer<KeyWritable, PriceAvgWritable, KeyWritable, DoubleWritable> {
        public void reduce(KeyWritable key, Iterable<PriceAvgWritable> values, Context con)
                throws IOException, InterruptedException {

            // logica do reduce:
            // recebe diferentes objetos compostos (temperatura, qtd)
            // somar as temperaturas e somar as qtds
            double somaPrice = 0;
            int somaTotal = 0;
            for (PriceAvgWritable o : values){
                somaPrice += o.getPrice();
                somaTotal += o.getCount();
            }
            // calcular a media
            double media = somaPrice / somaTotal;

            // salvando o resultado
            con.write(key, new DoubleWritable(media));


        }
    }

}