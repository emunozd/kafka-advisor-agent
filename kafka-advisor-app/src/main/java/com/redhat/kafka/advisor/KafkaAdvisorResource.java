package com.redhat.kafka.advisor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.output.Response;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Path("/")
public class KafkaAdvisorResource {

    @Inject Template advisor;
    @Inject ChatLanguageModel chatModel;
    @Inject ObjectMapper mapper;

    @ConfigProperty(name = "kafka.advisor.prompt.path",
                    defaultValue = "classpath:system-prompt.txt")
    String promptPath;

    private static final java.util.Set<String> SUPPORTED_KINDS =
        java.util.Set.of("Kafka", "KafkaTopic", "KafkaUser", "KafkaMirrorMaker2");

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance index() {
        return advisor.instance();
    }

    @POST
    @Path("/analyze")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public jakarta.ws.rs.core.Response analyze(AnalyzeRequest request) {
        try {
            String detectedKind = detectKind(request.yaml());

            if (!SUPPORTED_KINDS.contains(detectedKind)) {
                ObjectNode result = mapper.createObjectNode();
                result.put("recommendations",
                    "• Detected kind: " + detectedKind + " — not supported.\n" +
                    "• Supported kinds: Kafka, KafkaTopic, KafkaUser, KafkaMirrorMaker2.\n" +
                    "• Please paste a valid AMQ Streams CRD.");
                result.put("yaml",
                    "# kind: " + detectedKind + " is not supported.\n" +
                    "# Supported: Kafka, KafkaTopic, KafkaUser, KafkaMirrorMaker2");
                return jakarta.ws.rs.core.Response.ok(result).build();
            }

            String systemPrompt = loadSystemPrompt(detectedKind);
            String userMessage  = "Optimization goal: " + request.goal() +
                "\n\nInput CRD YAML:\n" + request.yaml() +
                "\n\nAnalyze this " + detectedKind + " CRD and provide the optimized configuration.";

            dev.langchain4j.data.message.SystemMessage sys =
                dev.langchain4j.data.message.SystemMessage.from(systemPrompt);
            dev.langchain4j.data.message.UserMessage usr =
                dev.langchain4j.data.message.UserMessage.from(userMessage);

            Response<AiMessage> resp = chatModel.generate(sys, usr);
            String raw = resp.content().text().trim();
            raw = raw.replaceAll("(?s)```json\\s*", "").replaceAll("```", "").trim();

            ObjectNode result = mapper.createObjectNode();
            result.put("recommendations", extractRecommendations(raw));
            result.put("yaml", extractYaml(raw));

            return jakarta.ws.rs.core.Response.ok(result).build();

        } catch (Exception e) {
            ObjectNode err = mapper.createObjectNode();
            err.put("recommendations", "Backend error: " + e.getMessage());
            err.put("yaml", "# Error processing response");
            return jakarta.ws.rs.core.Response.ok(err).build();
        }
    }

    private String detectKind(String yaml) {
        Pattern p = Pattern.compile("^kind:\\s*(\\S+)", Pattern.MULTILINE);
        Matcher m = p.matcher(yaml);
        return m.find() ? m.group(1).trim() : "Unknown";
    }

    private String extractRecommendations(String raw) {
        try {
            JsonNode json = mapper.readTree(raw);
            if (json.has("recommendations")) {
                JsonNode rec = json.get("recommendations");
                if (rec.isTextual()) return rec.asText();
                if (rec.isArray()) {
                    StringBuilder sb = new StringBuilder();
                    for (JsonNode item : rec) {
                        String line = item.asText().trim();
                        if (!line.startsWith("•") && !line.startsWith("-")) sb.append("• ");
                        sb.append(line).append("\n");
                    }
                    return sb.toString().trim();
                }
            }
        } catch (Exception ignored) {}

        StringBuilder sb = new StringBuilder();
        for (String line : raw.split("\n")) {
            String t = line.trim();
            if (t.startsWith("•") || t.startsWith("-") || t.startsWith("*")) {
                sb.append(t).append("\n");
            }
        }
        return sb.length() > 0 ? sb.toString().trim()
             : "See optimized YAML for changes applied.";
    }

    private String extractYaml(String raw) {
        try {
            JsonNode json = mapper.readTree(raw);
            if (json.has("yaml")) {
                return cleanYaml(json.get("yaml").asText());
            }
        } catch (Exception ignored) {}

        Pattern p = Pattern.compile(
            "(apiVersion:\\s*kafka\\.strimzi\\.io[\\s\\S]*)", Pattern.MULTILINE);
        Matcher m = p.matcher(raw);
        if (m.find()) {
            return cleanYaml(m.group(1));
        }

        int idx = raw.indexOf("\"yaml\":");
        if (idx >= 0) {
            String after = raw.substring(idx + 7).trim()
                .replaceAll("^[\"\\|\\s]+", "");
            return cleanYaml(after);
        }

        return "# Could not extract YAML.\n# See recommendations panel.";
    }

    private String cleanYaml(String raw) {
        String result = raw.replaceAll("\\\\([ ]{1,8})", "\n$1");
        result = result
            .replace("\\n", "\n")
            .replace("\\\"", "\"")
            .replace("\\t", "  ");
        result = result
            .replaceAll("\\\\\\s*\n", "\n")
            .replaceAll("\\\\\\s*$", "");

        String[] lines = result.split("\n");
        int last = lines.length - 1;
        while (last >= 0 && lines[last].trim().matches("[\"{}\\[\\]]+")
               && !lines[last].trim().equals("{}")) {
            last--;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i <= last; i++) {
            if (i > 0) sb.append("\n");
            sb.append(lines[i]);
        }
        result = sb.toString();
        result = result.replaceAll("(\\{\\})[\"\\}]+$", "$1");
        result = result.replaceAll("([^{])[\"\\}]{2,}$", "$1");
        result = result.replaceAll("\n{3,}", "\n\n").trim();

        return result;
    }

    private String loadSystemPrompt(String kind) throws IOException {
        String fileName = switch (kind) {
            case "KafkaUser"         -> "kafkauser-prompt.txt";
            case "KafkaMirrorMaker2" -> "mm2-prompt.txt";
            default                  -> "system-prompt.txt";
        };

        // Try mounted ConfigMap path first
        java.nio.file.Path base = Paths.get(promptPath).getParent();
        if (base != null) {
            java.nio.file.Path specific = base.resolve(fileName);
            if (Files.exists(specific)) return Files.readString(specific);
        }

        // Fallback to the default prompt file
        java.nio.file.Path defaultPath = Paths.get(promptPath);
        if (Files.exists(defaultPath)) return Files.readString(defaultPath);

        // Final fallback to classpath
        try (var is = getClass().getResourceAsStream("/" + fileName)) {
            if (is != null) return new String(is.readAllBytes());
        }
        try (var is = getClass().getResourceAsStream("/system-prompt.txt")) {
            if (is != null) return new String(is.readAllBytes());
        }
        throw new IOException("System prompt not found for kind: " + kind);
    }

    public record AnalyzeRequest(String goal, String yaml) {}
}
