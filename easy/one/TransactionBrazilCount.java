package TDE.easy.one;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class TransactionBrazilCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        j.setJarByClass(TransactionBrazilCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);
        j.setCombinerClass(CombineForWordCount.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class); //tipo da chave de saida do map
        j.setMapOutputValueClass(IntWritable.class); // tipo do valor de saida do map
        j.setOutputKeyClass(Text.class); // tipo da chave de saida do reduce
        j.setOutputValueClass(IntWritable.class); //tipo do valor de saida do reduce

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input); //arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); //arquivo de saida

        // lanca o job e aguarda sua execucao
        //System.exit(j.waitForCompletion(true) ? 0 : 1);
        j.waitForCompletion(false);


    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
          /* Soma os valores para definir o número de vezes que o país apareceu */
        //chave de entrada do map e um offset
        //value conteudo do arquivo
        /**
         *1o tipo: tipo da chave de entrada(LongWritter)
         *2o tipo: tipo do valor de entrada(Text)
         *3o tipo: tipo da chave de entrada(palavra, Text)
         *4o tipo: tipo da valor de entrada(1, int -> IntWritable)
         */
           /* Recebe valor e transforma para string, separa os valores por ";" utilizando split colocando-os em
            um array, inicializa a variável n. O if verifica se a coluna de países (columns[0]) é equivalente
            a Brazil, para assim identificar a chave como "Brazil" e identificá-lo com "1" por default pela
            quantidade */
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //Obtendo o conteudo da linha
            String linha = value.toString();

            //Quebrando a linha em palavras
            String[] transacao = linha.split(";");

            //chave
            String pais = transacao[0];
            if(Objects.equals(pais, "Brazil")){
                Text chave = new Text(pais);
                IntWritable valor = new IntWritable(1);
                //Enviando os dados
                con.write(chave, valor);

            }
        }
    }

    /**
     * /*
     *1o tipo: tipo da chave de saida do map(LongWritter)
     *2o tipo: tipo do valor de saida do map(Text)
     *3o tipo: tipo da chave de saida do reduce
     *4o tipo: tipo da valor de saida do reduce
     */
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int soma = 0;
            //loop para iterar sobre os valores de entrada e somar eles
            for (IntWritable v: values){
                soma += v.get();
            }

            // salvando o resultado em disco
            con.write(key, new IntWritable(soma));

        }
    }

    public static class CombineForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;

            for(IntWritable v: values){
                soma += v.get();
            }

            con.write(key, new IntWritable(soma));
        }
    }


}
