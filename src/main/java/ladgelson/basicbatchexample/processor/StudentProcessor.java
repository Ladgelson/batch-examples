package ladgelson.basicbatchexample.processor;

import ladgelson.basicbatchexample.model.Student;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class StudentProcessor implements ItemProcessor<Student, Student> {

    private final Logger logger = LoggerFactory.getLogger(StudentProcessor.class);
    @Override
    public Student process(Student student) throws Exception {
        logger.info("Processing...");
        final String firstName = student.getFirstname().toUpperCase();
        final String lastName = student.getLastname().toUpperCase();
        return new Student(student.getId(), firstName, lastName, student.getEmail());
    }
}
