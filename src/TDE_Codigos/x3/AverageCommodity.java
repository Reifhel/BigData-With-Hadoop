package TDE_Codigos.x3;

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

import TDE_Codigos.x3.Writables.CommAvgWritable;

import java.io.IOException;

public class AverageCommodity {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/TDE/x3.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "Media");


        // Registrar as classes
        j.setJarByClass(AverageCommodity.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(CommAvgWritable.class);
        // REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // rodar :)
        j.waitForCompletion(false);

    }

    public static class MapForAverage extends Mapper<LongWritable, Text, Text, CommAvgWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // funcao chamada automaticamente por linha do arquivo

            // obtendo a linha
            String linha = value.toString();

            if(!linha.startsWith("country_or_area")){

                // quebrando em colunas
                String colunas[] = linha.split(";");
    
    
                String year = colunas[1];
                float commodity = Float.parseFloat(colunas[5]);
                int qtd = 1;

                CommAvgWritable media = new CommAvgWritable(commodity, qtd);
    
                // enviando dados no formato (chave,valor) para o reduce
                con.write(new Text(year), media);
            }
            



        }
    }

    public static class CombineForAverage extends Reducer<Text, CommAvgWritable, Text, CommAvgWritable>{
        public void reduce(Text key, Iterable<CommAvgWritable> values, Context con)
                throws IOException, InterruptedException {

            // somar as temperaturas e as qtds para cada chave
            float somaValues = 0.0f;
            int somaTotal = 0;
            for(CommAvgWritable o : values){
                somaValues += o.getSomaCommodity();
                somaTotal += o.getTotal();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new CommAvgWritable(somaValues, somaTotal));


        }
    }


    public static class ReduceForAverage extends Reducer<Text, CommAvgWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<CommAvgWritable> values, Context con)
                throws IOException, InterruptedException {

            // logica do reduce:
            // recebe diferentes objetos compostos (temperatura, qtd)
            // somar as temperaturas e somar as qtds
            float somaCommodity = 0.0f;
            int somaTotal = 0;
            for (CommAvgWritable o : values){
                somaCommodity += o.getSomaCommodity();
                somaTotal += o.getTotal();
            }
            // calcular a media
            float media = somaCommodity / somaTotal;

            // salvando o resultado
            con.write(key, new FloatWritable(media));


        }
    }

}