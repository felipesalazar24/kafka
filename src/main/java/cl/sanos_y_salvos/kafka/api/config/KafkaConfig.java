package cl.sanos_y_salvos.kafka.api.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    
    @Value ("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean 
    public AdminClient KafkaAdminClient() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(configs);
    }

    @Bean
    public NewTopic petsEventsTopic() {
        return TopicBuilder.name("sanosysalvos.pets.events")
                .partitions(3)
                .replicas(2)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(7L * 24 * 60 * 60 * 1000)) // 7 dias
                .build();
    }

    @Bean
    public NewTopic analyticsAuditTopic() {
        return TopicBuilder.name("sanosysalvos.analytics.audit")
                .partitions(3)
                .replicas(2)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(30L * 24 * 60 * 60 * 1000)) // 30 dias
                .build();
    }
}
