package cl.sanos_y_salvos.kafka.api.service;

import cl.sanos_y_salvos.kafka.api.dto.ConsumerGroupResponse;
import cl.sanos_y_salvos.kafka.api.dto.CreateTopicRequest;
import cl.sanos_y_salvos.kafka.api.dto.TopicDetailResponse;
import cl.sanos_y_salvos.kafka.api.dto.UpdateTopicConfigRequest;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaService {
    
private final AdminClient adminClient;

    public KafkaService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public TopicDetailResponse createTopic(CreateTopicRequest request) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(request.name(), request.partitions(), request.replicationFactor());
        Map<String, String> configs = new HashMap<>();

        if (request.retentionMs() != null) {
            configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(request.retentionMs()));
        }
        if (request.cleanupPolicy() != null && !request.cleanupPolicy().isBlank()) {
            configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, request.cleanupPolicy());
        }
        newTopic.configs(configs);

        adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        return getTopicDetails(request.name());
    }

    public List<TopicDetailResponse> listTopics() throws ExecutionException, InterruptedException {
        Set<String> topicNames = adminClient.listTopics().names().get();
        List<TopicDetailResponse> result = new ArrayList<>();

        for (String name : topicNames) {
            if (!name.startsWith("__")) { // Filtrar tópicos internos de Kafka
                result.add(getTopicDetails(name));
            }
        }
        return result;
    }

    public TopicDetailResponse getTopicDetails(String topicName) throws ExecutionException, InterruptedException {
        TopicDescription desc = adminClient.describeTopics(Collections.singletonList(topicName))
                .allTopicNames().get().get(topicName);

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Config config = adminClient.describeConfigs(Collections.singletonList(resource))
                .all().get().get(resource);

        Map<String, String> relevantConfigs = new HashMap<>();
        for (ConfigEntry entry : config.entries()) {
            if (entry.name().equals(TopicConfig.RETENTION_MS_CONFIG)
                    || entry.name().equals(TopicConfig.CLEANUP_POLICY_CONFIG)
                    || entry.name().equals(TopicConfig.SEGMENT_MS_CONFIG)) {
                relevantConfigs.put(entry.name(), entry.value());
            }
        }

        int partitions = desc.partitions().size();
        int replicationFactor = desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size();

        return new TopicDetailResponse(topicName, partitions, replicationFactor, relevantConfigs);
    }

    public TopicDetailResponse updateTopic(String topicName, UpdateTopicConfigRequest request) throws ExecutionException, InterruptedException {
        // 1. Aumentar particiones si se solicitó (Kafka solo permite aumentar, nunca reducir)
        if (request.newPartitions() != null) {
            TopicDescription current = adminClient.describeTopics(Collections.singletonList(topicName))
                    .allTopicNames().get().get(topicName);
            if (request.newPartitions() > current.partitions().size()) {
                adminClient.createPartitions(Map.of(topicName, NewPartitions.increaseTo(request.newPartitions()))).all().get();
            }
        }

        // 2. Modificar configuraciones (retention.ms / cleanup.policy)
        Collection<AlterConfigOp> ops = new ArrayList<>();
        if (request.retentionMs() != null) {
            ops.add(new AlterConfigOp(
                    new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(request.retentionMs())),
                    AlterConfigOp.OpType.SET
            ));
        }
        if (request.cleanupPolicy() != null && !request.cleanupPolicy().isBlank()) {
            ops.add(new AlterConfigOp(
                    new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, request.cleanupPolicy()),
                    AlterConfigOp.OpType.SET
            ));
        }

        if (!ops.isEmpty()) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            adminClient.incrementalAlterConfigs(Map.of(resource, ops)).all().get();
        }

        return getTopicDetails(topicName);
    }

    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
    }

    public List<ConsumerGroupResponse> listConsumerGroups() throws ExecutionException, InterruptedException {
        Collection<ConsumerGroupListing> listings = adminClient.listConsumerGroups().all().get();
        List<String> groupIds = listings.stream()
                .map(ConsumerGroupListing::groupId)
                .toList();

        if (groupIds.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, ConsumerGroupDescription> descriptions = adminClient.describeConsumerGroups(groupIds).all().get();
        List<ConsumerGroupResponse> responses = new ArrayList<>();

        for (ConsumerGroupDescription desc : descriptions.values()) {
            Set<String> topics = desc.members().stream()
                    .flatMap(member -> member.assignment().topicPartitions().stream())
                    .map(TopicPartition::topic)
                    .collect(Collectors.toSet());

            responses.add(new ConsumerGroupResponse(
                    desc.groupId(),
                    desc.state().toString(),
                    desc.members().size(),
                    topics
            ));
        }
        return responses;
    }
}
