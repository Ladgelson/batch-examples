package ladgelson.basicbatchexample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class BasicBatchExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(BasicBatchExampleApplication.class, args);
	}

}
