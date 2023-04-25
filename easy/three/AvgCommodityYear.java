package TDE.easy.three;

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

public class AvgCommodityYear {

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
        Job j = new Job(c, "avgCommodityPerYear");

        //registro de classes
        j.setJarByClass(AvgCommodityYear.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(CommodityYearWritable.class); //tipo da chave de saida do map
        j.setMapOutputValueClass(AvgCommodityWritable.class); // tipo do valor de saida do map
        j.setOutputKeyClass(CommodityYearWritable.class); // tipo da chave de saida do reduce
        j.setOutputValueClass(FloatWritable.class); //tipo do valor de saida do reduce

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input); //arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); //arquivo de saida

        // lanca o job e aguarda sua execucao
        //System.exit(j.waitForCompletion(true) ? 0 : 1);
        j.waitForCompletion(false);


    }


    public static class MapForAverage extends Mapper<LongWritable, Text, CommodityYearWritable, AvgCommodityWritable> {
            /* Recebe valor e transforma para string, separa os valores por ";" utilizando split colocando-os em
            um array, inicializa a variável n. O if verifica se é o cabeçalho (ignora primeira linha), coleta
            os years, as commodities, e os trades. Cria um objeto de commodities por ano, insere trade em
            AvgWritable juntamente com n (1). E depois, será calculada a média de valores de commodity/ano */

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            if (!linha.contains("country_or_area")){

            String colunas[] = linha.split(";");

            String year = colunas[1];
            String commodity = colunas[2];
            float quantidade = Float.parseFloat(colunas[5]);

            AvgCommodityWritable val = new AvgCommodityWritable(quantidade, 1);
            CommodityYearWritable chave = new CommodityYearWritable(commodity, year);

            //MEEDIA PER YEAR
            con.write(chave, val);

            }

        }
    }

    public static class CombineForAverage extends Reducer<CommodityYearWritable, AvgCommodityWritable, CommodityYearWritable, AvgCommodityWritable> {
        /* Junta os valores de AvgCommodityWritable para passar à o reduce */
        public void reduce(CommodityYearWritable key, Iterable<AvgCommodityWritable> values, Context con)
                throws IOException, InterruptedException {
            

            int somaYears = 0;
            float somaComodities = 0;



            for(AvgCommodityWritable o: values){
                somaComodities += o.getSomaQuantidade();
                somaYears += o.getN();

            }

            //salvando resultado chave = unica, valor = media
            con.write(key, new AvgCommodityWritable(somaComodities, somaYears));
        }
    }


    public static class ReduceForAverage extends Reducer<CommodityYearWritable, AvgCommodityWritable, CommodityYearWritable, DoubleWritable> {
        
        /* Junta os valores de AvgWritable e n, e divide os dois valores por n (average) */ 
        public void reduce(CommodityYearWritable key, Iterable<AvgCommodityWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaYears = 0;
            double somaComodities = 0;



            for(AvgCommodityWritable o: values){
                somaYears += o.getSomaQuantidade();
                somaComodities += o.getN();

            }

            double media = somaYears / somaComodities;
            //salvando resultado chave = unica, valor = media
            con.write(key, new DoubleWritable(media));
        }
    }
}
