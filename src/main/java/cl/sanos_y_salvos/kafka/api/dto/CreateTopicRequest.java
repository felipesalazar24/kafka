package cl.sanos_y_salvos.kafka.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record CreateTopicRequest(
        @NotBlank(message = "El nombre del tópico es obligatorio")
        @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre solo puede contener letras, números, puntos, guiones y guiones bajos")
        String name,

        @Min(value = 1, message = "Debe tener al menos 1 partición")
        @Max(value = 20, message = "Máximo 20 particiones permitidas")
        int partitions,

        @Min(value = 1, message = "El factor de replicación mínimo es 1")
        @Max(value = 3, message = "El factor de replicación no puede superar los brokers disponibles")
        short replicationFactor,

        @Min(value = 1000, message = "La retención mínima es 1000 ms (1 segundo)")
        Long retentionMs,

        @Pattern(regexp = "^(delete|compact|delete,compact)$", message = "cleanupPolicy debe ser 'delete', 'compact' o 'delete,compact'")
        String cleanupPolicy
) {}
    

