package com.person.register.steps;


import com.person.register.config.DataSourceConfig;
import com.person.register.entity.Pessoa;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class PersonNameStep {

    /* Alterando o step para cadastrar pessoas a partir de um .csv */

    @Autowired
    public DataSourceConfig dataSourceConfig;

    @Bean
    public Step teste(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){
        return  new StepBuilder("teste",jobRepository)
                .<Pessoa, Pessoa> chunk(100,platformTransactionManager)
                .reader(leituraArquivo())
                .writer(carregarPessoa(dataSourceConfig.appDataSource()))
                .transactionManager(platformTransactionManager)
                .build();
    }

    @Bean
    public ItemReader<Pessoa> leituraArquivo(){

        return new FlatFileItemReaderBuilder<Pessoa>()
                .name("reader")
                .resource(new FileSystemResource("arquivos/pessoas3.csv"))
                .comments("--")
                .delimited()
                .names("nome", "email", "dataNascimento", "idade", "id")
                .targetType(Pessoa.class)
                .build();
    }

    @Bean
    public ItemWriter<Pessoa> carregarPessoa(@Qualifier("appDataSource") DataSource dataSource){

        return new JdbcBatchItemWriterBuilder<Pessoa>()
                .dataSource(dataSource)
                .sql(
                        "INSERT INTO pessoa (id, nome, email, data_nascimento, idade) VALUES (:id, :nome, :email, :dataNascimento, :idade)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }


}

