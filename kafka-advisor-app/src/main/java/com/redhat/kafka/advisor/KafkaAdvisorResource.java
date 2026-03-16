package com.redhat.kafka.advisor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.input.Prompt;
import dev.langchain4j.model.input.PromptTemplate;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

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
    public Response analyze(AnalyzeRequest request) {
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

            dev.langchain4j.model.output.Response<dev.langchain4j.data.message.AiMessage> resp =
                chatModel.generate(sys, usr);

            String raw = resp.content().text().trim();

            // Strip markdown fences if model adds them
            if (raw.startsWith("```")) {
                raw = raw.replaceAll("```json\\n?|```\\n?", "").trim();
            }

            JsonNode json = mapper.readTree(raw);
            return Response.ok(json).build();

        } catch (Exception e) {
            return Response.serverError()
                .entity(Map.of("error", e.getMessage()))
                .build();
        }
    }

    private String loadSystemPrompt() throws IOException {
        java.nio.file.Path path = Paths.get(promptPath);
        if (Files.exists(path)) {
            return Files.readString(path);
        }
        // Fallback to classpath (dev mode)
        try (var is = getClass().getResourceAsStream("/system-prompt.txt")) {
            if (is != null) return new String(is.readAllBytes());
        }
        throw new IOException("System prompt not found at: " + promptPath);
    }

    public record AnalyzeRequest(String goal, String yaml) {}
}
