package TDE_Codigos.x6;

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

import TDE_Codigos.x6.Writables.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

public class LargestExport_Country {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        // arquivo de entrada
        Path input = new Path("./in/transactions_amostra.csv");
        Path intermediate = new Path("./output/TDE/x6/intermediate.tmp");
        // arquivo de saida
        Path output = new Path("./output/TDE/x6/result.txt");

        // Criando o primeiro job
        Job j1 = new Job(c, "parte1");

        j1.setJarByClass(LargestExport_Country.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);
        j1.setCombinerClass(CombinerEtapaA.class);


        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(AverageFlowWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(AverageFlowWritable.class);


        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // Rodo o job 1
        // argumentos: in/JY157487.1.fasta output/entropia.txt
        j1.waitForCompletion(false);

        // Configuracao do job 2
        Job j2 = new Job(c, "result");

        j2.setJarByClass(LargestExport_Country.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);
        j2.setCombinerClass(CombinerEtapaB.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(LargestCountryWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(LargestCountryWritable.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);
        j2.waitForCompletion(false);


    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, AverageFlowWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Pegando a linha e quebrando em caracteres
            String linha = value.toString();

            if (linha.startsWith("country_or_area")) {
                return;
            }

            String campos[] = linha.split(";");

            String pais = campos[0];
            String flow = campos[4];
            double qtd = Double.parseDouble(campos[5]);

            if (flow.equals("Export")) {

                AverageFlowWritable valor = new AverageFlowWritable();
                valor.setAverage(qtd);
                valor.setCount(1);

                con.write(new Text(pais), valor);
            }

        
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, AverageFlowWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<AverageFlowWritable> values, Context con)
                throws IOException, InterruptedException {
            // somando todas as ocorrencias
            
            Double total = 0.0;
            int qtd = 0;

            for (AverageFlowWritable v : values) {
                total += v.getAverage();
                qtd += v.getCount();
            }

            DoubleWritable media = new DoubleWritable(total / qtd);
       

            // escrevendo o arquivo de resultados
            con.write(key, media);
        }
    }

    public static class CombinerEtapaA extends Reducer<Text, AverageFlowWritable, Text, AverageFlowWritable> {
        public void reduce(Text key, Iterable<AverageFlowWritable> values, Context con)
                throws IOException, InterruptedException {
            // somando todas as ocorrencias
            
            Double total = 0.0;
            int qtd = 0;

            for(AverageFlowWritable o : values){
                total += o.getAverage();
                qtd += o.getCount();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new AverageFlowWritable(total, qtd));

            
        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, LargestCountryWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String campos[] = linha.split("\t");

            String pais = campos[0];
            double qtd = Double.parseDouble(campos[1]);

            LargestCountryWritable valor = new LargestCountryWritable();
            valor.setCountry(pais);
            valor.setValue(qtd);

            con.write(new Text("Largest Export Country"), valor);

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, LargestCountryWritable, Text, LargestCountryWritable> {
        public void reduce(Text key, Iterable<LargestCountryWritable> values, Context con)
                throws IOException, InterruptedException {

            Double maior = 0.0;
            String pais = "";

            for (LargestCountryWritable v : values) {

                if (v.getValue() > maior) {
                    maior = v.getValue();
                    pais = v.getCountry();
                }

            }

            LargestCountryWritable maiorMedia = new LargestCountryWritable(pais, maior);

            // escrevendo o arquivo de resultados
            con.write(new Text("Largest Export Country  ->"), maiorMedia);
        }

    }

    public static class CombinerEtapaB extends Reducer<Text, AverageFlowWritable, Text, LargestCountryWritable> {
        public void reduce(Text key, Iterable<LargestCountryWritable> values, Context con)
                throws IOException, InterruptedException {
            // somando todas as ocorrencias
            
            String pais = "";
            Double qtd = 0.0;

            for(LargestCountryWritable o : values){
                pais += o.getCountry();
                qtd += o.getValue();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new LargestCountryWritable(pais, qtd));

            
        }
    }
    
}
