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
import jakarta.ws.rs.core.Response.Status;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

@Path("/")
public class KafkaAdvisorResource {

    @Inject
    Template advisor;

    @Inject
    ChatLanguageModel chatModel;

    @Inject
    ObjectMapper mapper;

    @ConfigProperty(name = "kafka.advisor.prompt.path",
                    defaultValue = "classpath:system-prompt.txt")
    String promptPath;

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
            String systemPrompt = loadSystemPrompt();

            String userMessage = String.format(
                "Optimization goal: %s\n\nInput CRD YAML:\n%s\n\n" +
                "Analyze this Kafka CRD and provide the optimized configuration.",
                request.goal(), request.yaml()
            );

            dev.langchain4j.data.message.SystemMessage sys =
                dev.langchain4j.data.message.SystemMessage.from(systemPrompt);
            dev.langchain4j.data.message.UserMessage usr =
                dev.langchain4j.data.message.UserMessage.from(userMessage);

            Response<AiMessage> resp = chatModel.generate(sys, usr);
            String raw = resp.content().text().trim();

            // Strip markdown fences if model adds them
            raw = raw.replaceAll("(?s)```json\\s*", "").replaceAll("```", "").trim();

            // Try to parse as JSON
            JsonNode json;
            try {
                json = mapper.readTree(raw);
            } catch (Exception parseEx) {
                // Model returned malformed JSON — wrap it as plain text
                ObjectNode fallback = mapper.createObjectNode();
                fallback.put("recommendations", raw);
                fallback.put("yaml", "# Could not parse optimized YAML from model response.\n" +
                                     "# Raw response saved in recommendations panel.");
                return jakarta.ws.rs.core.Response.ok(fallback).build();
            }

            // Normalize: if recommendations is an array, join into string
            if (json.has("recommendations") && json.get("recommendations").isArray()) {
                StringBuilder sb = new StringBuilder();
                for (JsonNode item : json.get("recommendations")) {
                    String line = item.asText().trim();
                    if (!line.startsWith("•") && !line.startsWith("-")) {
                        sb.append("• ");
                    }
                    sb.append(line).append("\n");
                }
                ((ObjectNode) json).put("recommendations", sb.toString().trim());
            }

            return jakarta.ws.rs.core.Response.ok(json).build();

        } catch (Exception e) {
            return jakarta.ws.rs.core.Response
                .status(Status.INTERNAL_SERVER_ERROR)
                .entity(Map.of("error", e.getMessage()))
                .build();
        }
    }

    private String loadSystemPrompt() throws IOException {
        java.nio.file.Path path = Paths.get(promptPath);
        if (Files.exists(path)) {
            return Files.readString(path);
        }
        try (var is = getClass().getResourceAsStream("/system-prompt.txt")) {
            if (is != null) return new String(is.readAllBytes());
        }
        throw new IOException("System prompt not found at: " + promptPath);
    }

    public record AnalyzeRequest(String goal, String yaml) {}
}
