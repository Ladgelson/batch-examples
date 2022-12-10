package ladgelson.basicbatchexample.config;

import ladgelson.basicbatchexample.model.Student;
import ladgelson.basicbatchexample.processor.StudentProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class StudentBatchConfiguration {

    private final Logger logger = LoggerFactory.getLogger(StudentBatchConfiguration.class);
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private DataSource dataSource;

    @Bean
    public FlatFileItemReader<Student> readDataFromCsv() {
        // Create reader instance
        FlatFileItemReader<Student> reader = new FlatFileItemReader<>();

        // Set input file location
        reader.setResource(new FileSystemResource("C:\\Users\\Colaborador - Datum\\Desktop\\STUDY\\basic-batch-example\\src\\main\\resources\\csv\\input.csv"));

        // Set number of lines to skips. Use it if file has header rows.
        // reader.setLinesToSkip(1);

        // Configure how each line will be parsed and mapped to different values
        reader.setLineMapper(new DefaultLineMapper<>() {
            {
                // This columns new String[] {"id", "firstName", "lastName", "email"} in each row
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(Student.fields());
                    }
                });
                // Set values in Student class
                setFieldSetMapper(new BeanWrapperFieldSetMapper<>() {
                    {
                        setTargetType(Student.class);
                    }
                });
            }
        });
        return reader;
    }

    @Bean
    // An processor is a Class which implements ItemProcessor<I, O> and here we define how/what
    // we will make to transform what are we reading to pass it to our writer
    public StudentProcessor processor() {
        return new StudentProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Student> writeDataOnDatabase() {
        String insert = "insert into csvtodbdata (id, firstname, lastname, email) " +
                "values (:id, :firstname, :lastname, :email)";
        JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql(insert);
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return writer;
    }

    @Bean
    // Defines an step of my csv to csv job
    public Step csvToDatabaseStep() {
        // It works as ranges to be executed, so, if the chunk size is 2,
        // It will read, process and write 2 by 2.
        return stepBuilderFactory.get("csvToDatabaseStep")
                .<Student, Student>chunk(2)
                .reader(readDataFromCsv())
                .processor(processor())
                .writer(writeDataOnDatabase())
                .build();
    }

    @Bean
    // Defines an Job to be executed, very simple, just setting our chunk step and build
    public Job processStudentJob() {
        return jobBuilderFactory.get("csv-to-database-job")
                .incrementer(new RunIdIncrementer())
                .flow(csvToDatabaseStep())
                .end().build();
    }

}
