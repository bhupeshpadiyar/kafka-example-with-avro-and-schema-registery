package com.bhupesh.aws;

import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application  implements CommandLineRunner{

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		
		Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try(AdminClient adminClient = AdminClient.create(props)){
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topics = topicsResult.names().get();
            System.out.println(topics);
        }catch (Exception e){
            e.printStackTrace();
        }

	}

}
