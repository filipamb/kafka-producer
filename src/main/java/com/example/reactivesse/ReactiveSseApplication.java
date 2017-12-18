package com.example.reactivesse;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.file.dsl.Files;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.io.File;

@SpringBootApplication
@RestController
public class ReactiveSseApplication {

	@Bean
	SubscribableChannel filesChannel() {
		return MessageChannels.publishSubscribe().get();
	}

	@Bean
	IntegrationFlow integrationFlow(@Value("/home/filip/in") File in) {
		return IntegrationFlows.from(Files.inboundAdapter(in).autoCreateDirectory(true),
				poller -> poller.poller(spec -> spec.fixedDelay(1000L)))
				.transform(File.class, File:: getAbsolutePath)
				.channel(filesChannel())
				.get();
	}

	@GetMapping(value = "/files", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	Flux<String> files() {
		return Flux.create(sink -> {
			MessageHandler handler = msg -> sink.next(String.class.cast(msg.getPayload()));
			sink.onCancel(() -> {
                System.out.println("CANCELLED SOUT");
                sink.next("CANCELLED");
			    filesChannel().unsubscribe(handler);
            });
			filesChannel().subscribe(handler);
		});
	}

	@GetMapping("produce")
	String produce() throws Exception {
		KafkaProducerExample.runProducer(10);
		return "OK";
	}
	public static void main(String[] args) {
		SpringApplication.run(ReactiveSseApplication.class, args);
	}
}
