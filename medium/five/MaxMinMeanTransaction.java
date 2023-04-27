package tde_grupo.medium.five;

import org.apache.commons.io.FileUtils;
import tde_grupo.easy.four.AvgCommodityPriceWritable;
import tde_grupo.easy.four.AvgCommodityUnitYearCategWritable;
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

import java.io.File;
import java.io.IOException;

public class MaxMinMeanTransaction {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        FileUtils.deleteDirectory(new File(files[1]));
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
    /* Recebe valor e transforma para string, separa os valores por ";" utilizando split colocando-os em
            um array, inicializa a variável n. O if verifica se é o cabeçalho (ignora primeira linha), coleta
           os unit_type, year e price. Cria dois objetos de AvgTransactionWritable e TypeYearWritable e os escreve */
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            if (!linha.contains("country_or_area")){
                String colunas[] = linha.split(";");


                //obter commodities
                String unit_type = colunas[7];
                String year = colunas[1];
                double price = Double.parseDouble(colunas[5]);

                AvgTransactionWritable val   = new AvgTransactionWritable(price, 1, price, price);
                TypeYearWritable       chave = new TypeYearWritable(unit_type, year);

                //MEEDIA PER YEAR
                con.write(chave, val);
            }
        }
    }

    public static class CombineForAverage extends Reducer<AvgCommodityUnitYearCategWritable, AvgTransactionWritable, Text, AvgTransactionWritable> {
        /*
        Combina as somas de preços e N
         */
        public void reduce(Text key, Iterable<AvgTransactionWritable> values, Context con)
                throws IOException, InterruptedException {
            //reduce opera por chave

            double max       = Double.NEGATIVE_INFINITY;
            double min       = Double.POSITIVE_INFINITY;
            double accSums   = 0.0;
            int accNum      = 0;




            for(AvgTransactionWritable o: values){
                accSums += o.getSomaPrice();
                accNum += o.getN();

                if (o.getMax() > max){
                    max = o.getMax();
                }

                if (o.getMin() < min){
                    min = o.getMin();
                }
            }

            con.write(key, new AvgTransactionWritable(accSums, accNum, max, min));

        }
    }


    public static class ReduceForAverage extends Reducer<TypeYearWritable, AvgTransactionWritable, TypeYearWritable, MaxMinMeanWritable> {
        
        
        /*
            Instância 4 variáveis, max, min, as quais são respectivamente iniciadas com o menor múmero 
            float possível e maior número float possível, para que possam ser substituídos pelo primeiro maior ou menor número;
            Logo em seguida, temos os contadores de num e sum, para realizar o cálculo da média.
            Após isso, retornamos os valores de max, min, media.


         */
        public void reduce(TypeYearWritable key, Iterable<AvgTransactionWritable> values, Context con)
                throws IOException, InterruptedException {

            double max       = Double.NEGATIVE_INFINITY;
            double min       = Double.POSITIVE_INFINITY;
            double accSums   = 0.0;
            int accNum      = 0;




            for(AvgTransactionWritable o: values){
                accSums += o.getSomaPrice();
                accNum += o.getN();

                if (o.getMax() > max){
                    max = o.getMax();
                }

                if (o.getMin() < min){
                    min = o.getMin();
                }
            }



            double media = accSums / accNum;

            con.write(key, new MaxMinMeanWritable(max, min, media));
        }
    }
}

