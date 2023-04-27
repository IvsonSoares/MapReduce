    package tde_grupo.easy.four;

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
import java.util.Objects;

public class AvgPriceCommodity {

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
        Job j = new Job(c, "avgCommodityPrice");

        //registro de classes
        j.setJarByClass(AvgPriceCommodity.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(AvgCommodityUnitYearCategWritable.class); //tipo da chave de saida do map
        j.setMapOutputValueClass(AvgCommodityPriceWritable.class); // tipo do valor de saida do map
        j.setOutputKeyClass(AvgCommodityUnitYearCategWritable.class); // tipo da chave de saida do reduce
        j.setOutputValueClass(FloatWritable.class); //tipo do valor de saida do reduce

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input); //arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); //arquivo de saida

        j.waitForCompletion(false);


    }


    public static class MapForAverage extends Mapper<LongWritable, Text, AvgCommodityUnitYearCategWritable, AvgCommodityPriceWritable> {
             /* Recebe valor e transforma para string, separa os valores por ";" utilizando split colocando-os em
            um array, inicializa a variável n. O if verifica se é o cabeçalho (ignora primeira linha), coleta
            os years, as commodities, e os trades. Cria um objeto de commodities por ano, insere trade em
            AvgWritable juntamente com n (1). E depois, será calculada a média de valores de commodity/ano */

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            if (!linha.contains("country_or_area")){

                String colunas[] = linha.split(";");
                String commodity_code = colunas[2];
                String unit_type = colunas[7];
                String year = colunas[1];
                String category = colunas[9];
                String country_or_area    = colunas[0];
                String flow               = colunas[4];
                String country_filter = "Brazil";
                String flow_filter = "Export";
                float price = Float.parseFloat(colunas[5]);

                if (Objects.equals(country_or_area, country_filter) && Objects.equals(flow, flow_filter)) {

                    AvgCommodityPriceWritable         val   = new AvgCommodityPriceWritable(price, 1);
                    AvgCommodityUnitYearCategWritable chave = new AvgCommodityUnitYearCategWritable(commodity_code, unit_type, year, category);

                    con.write(chave, val);
                }
            }

        }
    }

    public static class CombineForAverage extends Reducer<AvgCommodityUnitYearCategWritable, AvgCommodityPriceWritable, Text, AvgCommodityPriceWritable> {
        /* COMBINER. Junta os valores de AvgCommodityPriceWritable e n*/
        public void reduce(Text key, Iterable<AvgCommodityPriceWritable> values, Context con)
                throws IOException, InterruptedException {
             

            float somaPrice = 0;
            int qtdComodities = 0;



            for(AvgCommodityPriceWritable o: values){
                somaPrice     += o.getSomaPrice();
                qtdComodities += o.getN();
            }

            //salvando resultado chave = unica, valor = media
            con.write(key, new AvgCommodityPriceWritable(somaPrice, qtdComodities));

        }
    }


    public static class ReduceForAverage extends Reducer<AvgCommodityUnitYearCategWritable, AvgCommodityPriceWritable, AvgCommodityUnitYearCategWritable, DoubleWritable> {
        /* Junta os valores de AvgWritable e n, e divide os dois valores por n (average) */
        public void reduce(AvgCommodityUnitYearCategWritable key, Iterable<AvgCommodityPriceWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaPrice = 0;
            double somaComodities = 0;



            for(AvgCommodityPriceWritable o: values){
                somaPrice += o.getSomaPrice();
                somaComodities += o.getN();
            }

            double media = somaPrice / somaComodities;
            //salvando resultado chave = unica, valor = media
            con.write(key, new DoubleWritable(media));
        }
    }
}
