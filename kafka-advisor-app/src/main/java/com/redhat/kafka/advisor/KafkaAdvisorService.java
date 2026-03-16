package com.redhat.kafka.advisor;

// This interface is intentionally minimal.
// The system prompt is loaded dynamically from the ConfigMap
// mounted at /deployments/config/prompt/system-prompt.txt
// See KafkaAdvisorResource.java for the actual AI invocation logic.
public class KafkaAdvisorService {
    // No-op placeholder — logic lives in KafkaAdvisorResource
}
