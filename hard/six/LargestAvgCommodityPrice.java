package tde_grupo.hard.six;

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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;

public class LargestAvgCommodityPrice {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        String temp_path = "output/TDE/hard/six/temp";
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path temp_output = new Path(temp_path);//new Path(files[1]);
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j_1 = new Job(c, "AvgPricePerCountry");

        //registro de classes
        j_1.setJarByClass(LargestAvgCommodityPrice.class);
        j_1.setMapperClass(MapForAverageA.class);
        j_1.setReducerClass(ReduceForAverageA.class);
        j_1.setCombinerClass(CombineForAverageA.class);

        // definicao dos tipos de saida
        j_1.setMapOutputKeyClass(Text.class); //tipo da chave de saida do map
        j_1.setMapOutputValueClass(AvgCommodityPriceWritable.class); // tipo do valor de saida do map
        j_1.setOutputKeyClass(Text.class); // tipo da chave de saida do reduce
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
        j_2.setJarByClass(LargestAvgCommodityPrice.class);
        j_2.setMapperClass(MapForAverageB.class);
        j_2.setReducerClass(ReduceForAverageB.class);
        j_2.setCombinerClass(CombineForAverageB.class);

        // definicao dos tipos de saida
        j_2.setMapOutputKeyClass(Text.class); //tipo da chave de saida do map
        j_2.setMapOutputValueClass(CountryQtdWritable.class); // tipo do valor de saida do map
        j_2.setOutputKeyClass(Text.class); // tipo da chave de saida do reduce
        j_2.setOutputValueClass(DoubleWritable.class); //tipo do valor de saida do reduce

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j_2, temp_output); //arquivo de entrada
        FileOutputFormat.setOutputPath(j_2, output); //arquivo de saida

        // lanca o job e aguarda sua execucao
        //System.exit(j.waitForCompletion(true) ? 0 : 1);
       j_2.waitForCompletion(false);


    }


    public static class MapForAverageA extends Mapper<LongWritable, Text, Text, AvgCommodityPriceWritable> {

        /**
         *  Recebe valor e transforma para string, separa os valores por ";" utilizando split colocando-os em
            um array, inicializa a variável n. O if verifica se é o cabeçalho (ignora primeira linha), coleta
           os country_or_area. flow, flow_filter e price. Cria dois objetos de AvgTransactionWritable e 
           TypeYearWritable e o escreve o preço no objeto AvgCommodityPriceWritable se ele for de flow export.
         */
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            if (!linha.contains("country_or_area")){

                String colunas[] = linha.split(";");

                String country_or_area    = colunas[0];
                String flow               = colunas[4];
                String flow_filter = "Export";
                long price = Long.parseLong(colunas[5]);

                if (Objects.equals(flow, flow_filter)) {

                    AvgCommodityPriceWritable val = new AvgCommodityPriceWritable(price, 1);

                    con.write(new Text(country_or_area), val);
                }
            }

        }
    }

    public static class CombineForAverageA extends Reducer<Text, AvgCommodityPriceWritable, Text, AvgCommodityPriceWritable> {
        /**
         * Como todo combiner de média, ele vai contabilizar o total do número que buscamos a média 
         * e a quantidade total de ocorrências.
         * 
         */
        public void reduce(Text key, Iterable<AvgCommodityPriceWritable> values, Context con)
                throws IOException, InterruptedException {
            //reduce opera por chave

            long somaPrice     = 0;
            int somaComodities = 0;


            for(AvgCommodityPriceWritable o: values){
                somaPrice      += o.getSomaPrice();
                somaComodities += o.getN();
            }

            con.write(key, new AvgCommodityPriceWritable(somaPrice, somaComodities));

        }
    }


    public static class ReduceForAverageA extends Reducer<Text, AvgCommodityPriceWritable, Text, DoubleWritable> {
        /**
         * Mesmo processo para realizar a média no reduce, contabilizará os preços e as quantidades
         *  para retornar a divisão de ambos, ou seja, a média.
         */
        public void reduce(Text key, Iterable<AvgCommodityPriceWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaPrice = 0;
            float somaComodities = 0;

            for(AvgCommodityPriceWritable o: values){
                somaPrice += o.getSomaPrice();
                somaComodities += o.getN();
            }

            double media = somaPrice / somaComodities;
            //salvando resultado chave = unica, valor = media
            con.write(key, new DoubleWritable(media));
        }
    }

    public static class MapForAverageB extends Mapper<LongWritable, Text, Text, CountryQtdWritable> {
        /**
         * Após obtermos as médias de todos, temos que mapear os dados já pré processados pelo reduce anterior;
         * Dessa forma, iremos dar split por \t, ou tab, pois é a forma que o reduce dividiu nossos dados.
         */
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            // Obtendo campos
            String campos[] = linha.split("\t");

            // Chave fixa para enviar todos para o mesmo reduce
            String chave = "todos";

            con.write(new Text(chave), new CountryQtdWritable(campos[0], Double.parseDouble(String.format("%f", Double.parseDouble(campos[1])))));

        }
    }

    public static class CombineForAverageB extends Reducer<Text, CountryQtdWritable, Text, CountryQtdWritable> {
        /**
         * Neste combiner, estaremos buscando o maior valor do map local, reduzindo o output do mesmo significativamente.
         * Já que cada Map antes do sort/shuffle retornará somente uma chave valor, invés de uma lista.
         * 
         * Para isso, definimos a variável max como double negative infinity, pois é o menor valor possível e quaisquer
         * valores serão maiores do que o mesmo. Dessa forma, iteramos no array e então, obtemos o país com a maior média.
         */
        public void reduce(Text key, Iterable<CountryQtdWritable> values, Context con)
                throws IOException, InterruptedException {
            //reduce opera por chave

            double max = Double.NEGATIVE_INFINITY;
            String country = "";

            for (CountryQtdWritable t: values) {
                if (t.getTotal() > max) {
                    max = t.getTotal();
                    country = t.getCountry();
                }
            }

                con.write(key, new CountryQtdWritable(country, max));
            }

        }


    public static class ReduceForAverageB extends Reducer<Text, CountryQtdWritable, Text, DoubleWritable> {
        /**
         * Por fim, no reduce, fazemos o último filtro para retornarmos o país com a maior média.
         */
        public void reduce(Text key, Iterable<CountryQtdWritable> values, Context con)
                throws IOException, InterruptedException {

            double max = Double.NEGATIVE_INFINITY;
            String country = "";

            for (CountryQtdWritable t: values) {
                if (t.getTotal() > max){
                    max = t.getTotal();
                    country = t.getCountry();
                }
            }

            con.write(new Text(country), new DoubleWritable(max));
        }
    }
}
