package TDE_Codigos.x7;

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

import TDE_Codigos.x7.Writables.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

public class MostCommercializedPerFlow {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        // arquivo de entrada
        Path input = new Path("./in/transactions_amostra.csv");
        Path intermediate = new Path("./output/TDE/x7/intermediate.tmp");
        // arquivo de saida
        Path output = new Path("./output/TDE/x7/result.txt");

        // Criando o primeiro job
        Job j1 = new Job(c, "parte1");

        j1.setJarByClass(MostCommercializedPerFlow.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);
        j1.setCombinerClass(CombinerEtapaA.class);


        j1.setMapOutputKeyClass(FlowCommodityWritable.class);
        j1.setMapOutputValueClass(FloatWritable.class);
        j1.setOutputKeyClass(FlowCommodityWritable.class);
        j1.setOutputValueClass(qtdOfCommodityWritable.class);


        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // Rodo o job 1
        // argumentos: in/JY157487.1.fasta output/entropia.txt
        j1.waitForCompletion(false);

        // // Configuracao do job 2
        Job j2 = new Job(c, "result");

        j2.setJarByClass(MostCommercializedPerFlow.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);
        j2.setCombinerClass(CombinerEtapaB.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(qtdOfCommodityWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(qtdOfCommodityWritable.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);
        j2.waitForCompletion(false);


    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, FlowCommodityWritable, FloatWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Pegando a linha e quebrando em caracteres
            String linha = value.toString();

            if (linha.startsWith("country_or_area")) {
                return;
            }

            String campos[] = linha.split(";");

            String flow = campos[4];
            String commodityName = campos[3];
            String year = campos[1];
            float qtd = Float.parseFloat(campos[8]);

            if (year.equals("2016")) {

                FlowCommodityWritable flowCommodity = new FlowCommodityWritable();
                flowCommodity.setFlow(flow);
                flowCommodity.setCommodity(commodityName);

                con.write(flowCommodity, new FloatWritable(qtd));

            }

        }
    }

    public static class ReduceEtapaA extends Reducer<FlowCommodityWritable, FloatWritable, FlowCommodityWritable, FloatWritable> {
        public void reduce(FlowCommodityWritable key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {
            // somando todas as ocorrencias
            
            Float total = 0.0f;


            for (FloatWritable v : values) {

                total += v.get();

            }

    

            // escrevendo o arquivo de resultados
            con.write(key, new FloatWritable(total));
        }
    }

    public static class CombinerEtapaA extends Reducer<FlowCommodityWritable, FloatWritable, FlowCommodityWritable, FloatWritable> {
        public void reduce(FlowCommodityWritable key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {
            // somando todas as ocorrencias
                           
            Float total = 0.0f;

            for (FloatWritable v : values) {

                total += v.get();

            }

            // escrevendo o arquivo de resultados
            con.write(key, new FloatWritable(total));
            
        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, qtdOfCommodityWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String campos[] = linha.split(";");

            String flow = campos[0];
            String commodityName = campos[1];
            float qtd = Float.parseFloat(campos[2]);

            qtdOfCommodityWritable valor = new qtdOfCommodityWritable();
            valor.setCommodity(commodityName);
            valor.setQtd(qtd);

            con.write(new Text(flow), valor);

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, qtdOfCommodityWritable, Text, qtdOfCommodityWritable> {
        public void reduce(Text key, Iterable<qtdOfCommodityWritable> values, Context con)
                throws IOException, InterruptedException {

                    // pegando o maior de cada tipo
                    Float maior = 0.0f;
                    String commodity = "";

                    for(qtdOfCommodityWritable o : values){
                        if(o.getQtd() > maior){
                            maior = o.getQtd();
                            commodity = o.getCommodity();
                        }
                    }

                    // escrevendo o arquivo de resultados
                    con.write(key, new qtdOfCommodityWritable(commodity, maior));

                }

    }

    public static class CombinerEtapaB extends Reducer<Text, qtdOfCommodityWritable, Text, qtdOfCommodityWritable> {
        public void reduce(Text key, Iterable<qtdOfCommodityWritable> values, Context con)
                throws IOException, InterruptedException {
            // somando todas as ocorrencias
            
            Float qtd = 0.0f;
            String commodity = "";

            for(qtdOfCommodityWritable o : values){
                if(o.getQtd() > qtd){
                    qtd = o.getQtd();
                    commodity = o.getCommodity();
                }
            }
            
            // escrevendo o arquivo de resultados
            con.write(key, new qtdOfCommodityWritable(commodity, qtd));

            
        }
    }
    
}
