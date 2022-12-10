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

@Configuration
@EnableBatchProcessing
public class StudentBatchConfiguration {

    private final Logger logger = LoggerFactory.getLogger(StudentBatchConfiguration.class);
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

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
        logger.info("Reader...");
        return reader;
    }

    @Bean
    // An processor is a Class which implements ItemProcessor<I, O> and here we define how/what
    // we will make to transform what are we reading to pass it to our writer
    public StudentProcessor processor() {
        return new StudentProcessor();
    }

    @Bean
    // Each step will have a writer, and our writer is a Class where we will write our output data
    // inside another file, after the transformation
    public FlatFileItemWriter<Student> writeDataOnCsv() {
        // Create writer instance
        FlatFileItemWriter<Student> writer = new FlatFileItemWriter<>();

        // Set output file location
        writer.setResource(new FileSystemResource("C:\\Users\\Colaborador - Datum\\Desktop\\STUDY\\basic-batch-example\\src\\main\\resources\\csv\\output.csv"));

        // All job repetitions should "append" to same output file
        // writer.setAppendAllowed(true);

        // Name field values sequence based on object properties

        // This is a field extractor for a java bean. Given an array of property names,
        // it will reflectively call getters on the item and return an array of all the values.
        BeanWrapperFieldExtractor<Student> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(Student.fields());

        // A LineAggregator implementation that converts an object into a delimited list of strings.
        // The default delimiter is a comma.
        DelimitedLineAggregator<Student> aggregator = new DelimitedLineAggregator<>();
        aggregator.setFieldExtractor(fieldExtractor);
        writer.setLineAggregator(aggregator);
        logger.info("Writer...");
        return writer;
    }

    @Bean
    // Defines an step of my csv to csv job
    public Step csvToCsvStep() {
        // It works as ranges to be executed, so, if the chunk size is 2,
        // It will read, process and write 2 by 2.
        return stepBuilderFactory.get("csvToCsvStep")
                .<Student, Student>chunk(2)
                .reader(readDataFromCsv())
                .processor(processor())
                .writer(writeDataOnCsv())
                .build();
    }

    @Bean
    // Defines an Job to be executed, very simple, just setting our chunk step and build
    public Job processStudentJob() {
        return jobBuilderFactory.get("processStudentJob")
                .incrementer(new RunIdIncrementer())
                .flow(csvToCsvStep())
                .end().build();
    }

}
