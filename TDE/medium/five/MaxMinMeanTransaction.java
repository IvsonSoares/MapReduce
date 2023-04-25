package TDE.medium.five;

import TDE.easy.four.AvgCommodityPriceWritable;
import TDE.easy.four.AvgCommodityUnitYearCategWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MaxMinMeanTransaction {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "MaxMinMeanTransaction");

        //registro de classes
        j.setJarByClass(MaxMinMeanTransaction.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(TypeYearWritable.class); //tipo da chave de saida do map
        j.setMapOutputValueClass(AvgTransactionWritable.class); // tipo do valor de saida do map
        j.setOutputKeyClass(TypeYearWritable.class); // tipo da chave de saida do reduce
        j.setOutputValueClass(MaxMinMeanWritable.class); //tipo do valor de saida do reduce

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input); //arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); //arquivo de saida

        // lanca o job e aguarda sua execucao
        //System.exit(j.waitForCompletion(true) ? 0 : 1);
        j.waitForCompletion(false);


    }


    public static class MapForAverage extends Mapper<LongWritable, Text, TypeYearWritable, AvgTransactionWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            if (!linha.contains("country_or_area")){
                String colunas[] = linha.split(";");


                //obter commodities
                String unit_type = colunas[7];
                String year = colunas[1];
                float price = Float.parseFloat(colunas[5]);

                AvgTransactionWritable val   = new AvgTransactionWritable(price, 1);
                TypeYearWritable       chave = new TypeYearWritable(unit_type, year);

                //MEEDIA PER YEAR
                con.write(chave, val);
            }
        }
    }

    public static class CombineForAverage extends Reducer<AvgCommodityUnitYearCategWritable, AvgTransactionWritable, Text, AvgTransactionWritable> {
        public void reduce(Text key, Iterable<AvgCommodityPriceWritable> values, Context con)
                throws IOException, InterruptedException {
            //reduce opera por chave

            float somaPrice = 0;
            int   somaQtd   = 0;


            for(AvgCommodityPriceWritable o: values){
                somaPrice += o.getSomaPrice();
                somaQtd   += o.getN();
            }

            con.write(key, new AvgTransactionWritable(somaPrice, somaQtd));

        }
    }


    public static class ReduceForAverage extends Reducer<TypeYearWritable, AvgTransactionWritable, TypeYearWritable, MaxMinMeanWritable> {
        public void reduce(TypeYearWritable key, Iterable<AvgTransactionWritable> values, Context con)
                throws IOException, InterruptedException {

            float max       = 0;
            float min       = (float) Double.POSITIVE_INFINITY;
            float somaPrice = 0;
            int   somaQtd   = 0;



            for(AvgTransactionWritable o: values){
                somaPrice += o.getSomaPrice();
                somaQtd += o.getN();

                if (o.getSomaPrice() > max){
                    max = o.getSomaPrice();
                }

                if (o.getSomaPrice() < min){
                    min = o.getSomaPrice();
                }
            }



            float media = somaPrice / somaQtd;

            con.write(key, new MaxMinMeanWritable(max, min, media));
        }
    }
}

