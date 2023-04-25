package TDE.hard.seven;

import TDE.hard.six.AvgCommodityPriceWritable;
import TDE.hard.six.CountryQtdWritable;
import advanced.entropy.BaseQtdWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.LinkedList;
import java.util.Objects;

public class MostCommercializedCommodity {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        String temp_path = "output/TDE/hard/seven/temp";
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path temp_output = new Path(temp_path);//new Path(files[1]);
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j_1 = new Job(c, "MostCommercializedCommodity");

        //registro de classes
        j_1.setJarByClass(MostCommercializedCommodity.class);
        j_1.setMapperClass(MostCommercializedCommodity.MapForAverageA.class);
        j_1.setReducerClass(MostCommercializedCommodity.ReduceForAverageA.class);
        j_1.setCombinerClass(MostCommercializedCommodity.CombineForAverageA.class);

        // definicao dos tipos de saida
        j_1.setMapOutputKeyClass(CommodityFlowWritable.class); //tipo da chave de saida do map
        j_1.setMapOutputValueClass(QuantityCommodityWritable.class); // tipo do valor de saida do map
        j_1.setOutputKeyClass(CommodityFlowWritable.class); // tipo da chave de saida do reduce
        j_1.setOutputValueClass(DoubleWritable.class); //tipo do valor de saida do reduce

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j_1, input); //arquivo de entrada
        FileOutputFormat.setOutputPath(j_1, temp_output); //arquivo de saida

        // lanca o job e aguarda sua execucao
        //System.exit(j.waitForCompletion(true) ? 0 : 1);
        j_1.waitForCompletion(false);

        // Job2

        // criacao do job e seu nome
        Job j_2 = new Job(c, "LargestAvg");

        //registro de classes
        j_2.setJarByClass(MostCommercializedCommodity.class);
        j_2.setMapperClass(MostCommercializedCommodity.MapForAverageB.class);
        j_2.setReducerClass(MostCommercializedCommodity.ReduceForAverageB.class);
        j_2.setCombinerClass(MostCommercializedCommodity.CombineForAverageB.class);

        // definicao dos tipos de saida
        j_2.setMapOutputKeyClass(Text.class); //tipo da chave de saida do map
        j_2.setMapOutputValueClass(QuantityCommodityWritable.class); // tipo do valor de saida do map
        j_2.setOutputKeyClass(Text.class); // tipo da chave de saida do reduce
        j_2.setOutputValueClass(QuantityCommodityWritable.class); //tipo do valor de saida do reduce

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j_2, temp_output); //arquivo de entrada
        FileOutputFormat.setOutputPath(j_2, output); //arquivo de saida

        // lanca o job e aguarda sua execucao
        //System.exit(j.waitForCompletion(true) ? 0 : 1);
        j_2.waitForCompletion(false);

    }


    public static class MapForAverageA extends Mapper<LongWritable, Text, CommodityFlowWritable, QuantityCommodityWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            if (!linha.contains("country_or_area")){

                String colunas[] = linha.split(";");

                //obter commodities
                String year       = colunas[1];
                String year_filer = "2016";
                String commodity  = colunas[3];
                String flow       = colunas[4];
                double quantity   = Double.parseDouble(colunas[8]);

                //total de commodities comercializadas por tipo
                if (Objects.equals(year, year_filer)) {


                    QuantityCommodityWritable val = new QuantityCommodityWritable(quantity, commodity);
                    CommodityFlowWritable chave = new CommodityFlowWritable(commodity, flow);

                    con.write(chave, val);
                }
            }

        }
    }

    public static class CombineForAverageA extends Reducer<CommodityFlowWritable, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            //reduce opera por chave

            int somaComodities = 0;


            for(DoubleWritable v: values){
                somaComodities += v.get();
            }

            //salvando resultado chave = unica, valor = media
            con.write(key, new DoubleWritable(somaComodities));

        }
    }


    public static class ReduceForAverageA extends Reducer<CommodityFlowWritable, QuantityCommodityWritable, CommodityFlowWritable, DoubleWritable> {
        public void reduce(CommodityFlowWritable key, Iterable<QuantityCommodityWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaQtd = 0;

            for(QuantityCommodityWritable o: values){
                somaQtd += o.getQuantity();
            }

            con.write(key, new DoubleWritable(somaQtd));
        }
    }

    public static class MapForAverageB extends Mapper<LongWritable, Text, Text, QuantityCommodityWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            // Obtendo campos
            String campos[] = linha.split("\t");

            String commodity = campos[0];
            String flow      = campos[1];
            double qtd       = Double.parseDouble(campos[2]);

            // Chave fixa para enviar todos para o mesmo reduce
            QuantityCommodityWritable val = new QuantityCommodityWritable(qtd, commodity);

            con.write(new Text(flow), val);

        }
    }

    public static class CombineForAverageB extends Reducer<Text, QuantityCommodityWritable, Text, QuantityCommodityWritable> {
        public void reduce(Text key, Iterable<QuantityCommodityWritable> values, Context con)
                throws IOException, InterruptedException {
            //reduce opera por chave


            double max = 0;
            String commodity = "";

            for (QuantityCommodityWritable t: values) {
                if (t.getQuantity() > max){
                    max       = t.getQuantity();
                    commodity = t.getCommodity();
                }
            }

            con.write(key, new QuantityCommodityWritable(max, commodity));

        }
    }


    public static class ReduceForAverageB extends Reducer<Text, QuantityCommodityWritable, Text, QuantityCommodityWritable> {
        public void reduce(Text key, Iterable<QuantityCommodityWritable> values, Context con)
                throws IOException, InterruptedException {

            double max = 0;
            String commodity = "";

            for (QuantityCommodityWritable t: values) {
                if (t.getQuantity() > max){
                    max       = t.getQuantity();
                    commodity = t.getCommodity();
                }
            }

            con.write(key, new QuantityCommodityWritable(max, commodity));
        }
    }
}
