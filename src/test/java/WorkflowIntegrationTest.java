import bio.cosy.featurecloud.orchestration.api.orchestration.workflow.StartWorkflowNodeDTO;
import bio.cosy.featurecloud.orchestration.api.orchestration.workflow.actions.WorkflowNodeInputActionDTO;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Frame;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
@QuarkusTestResource(LearningApiTestResource.class)
public class WorkflowIntegrationTest {

    private static final String TEST_IMAGE = "nginx:alpine";
    private static final long WORKFLOW_ID = 1L;

    @Inject
    DockerClient dockerClient;


    @Test
    public void fullWorkflowScenario(@TempDir Path tempDir) throws IOException {
        // 1. Start workflow step 1
        Long workflowNodeId = 1L;
        StartWorkflowNodeDTO step1 = getStartDTO(workflowNodeId);
        step1.setIsFistNode(true);

        CreateContainerResponse resp1 = given()
                .contentType(ContentType.JSON)
                .body(step1)
                .when().post("/container/workflow")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("Id", notNullValue())
                .extract().as(CreateContainerResponse.class);
        Assertions.assertNotNull(resp1.getId(), "Step 1 container should be created");

        // 2. Upload two files into the workflow input volume
        // File 1
        Path file1 = tempDir.resolve("test_1.txt");
        Files.writeString(file1, "Hello Workflow 1!");
        given()
                .multiPart("file", file1.toFile())
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .when().post("/volume/workflow/{workflowId}/node/{workflowNodeId}/upload",
                        WORKFLOW_ID,
                        workflowNodeId)
                .then()
                .statusCode(anyOf(is(200), is(204)));

        // File 2
        Path file2 = tempDir.resolve("test_2.txt");
        Files.writeString(file2, "Hello Workflow 2!");
        given()
                .multiPart("file", file2.toFile())
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .when().post("/volume/workflow/{workflowId}/node/{workflowNodeId}/upload",
                        WORKFLOW_ID,
                        workflowNodeId)
                .then()
                .statusCode(anyOf(is(200), is(204)));

        // Move both files to the output directory
        given()
                .contentType(MediaType.APPLICATION_JSON)
                .when().put("/volume/workflow/{workflowId}/node/{workflowNodeId}/move-to-output",
                        WORKFLOW_ID,
                        workflowNodeId)
                .then()
                .statusCode(anyOf(is(200), is(204)));

        // 3. Start workflow step 2 with file 1, rename to processed_test_1.txt
        workflowNodeId += 1;
        StartWorkflowNodeDTO step2 = getStartDTO(workflowNodeId);
        step2.setInputs(getActionDTOs(1L, false, "test_1.txt"));
        startStep(step2);


        // 4. Start workflow step 3 with file 2, rename to processed_test_2.txt
        workflowNodeId += 1;
        StartWorkflowNodeDTO step3 = getStartDTO(workflowNodeId);
        step3.setInputs(getActionDTOs(1L, true, "test_2.txt"));
        startStep(step3);

        // 4. Start workflow step 4, requesting both processed files as inputs
        workflowNodeId += 1;
        StartWorkflowNodeDTO step4 = getStartDTO(workflowNodeId);
        List<WorkflowNodeInputActionDTO> inputs = getActionDTOs(2L, true, "processed_test_1.txt");
        inputs.addAll(getActionDTOs(3L, true, "processed_test_2.txt"));
        step4.setInputs(inputs);
        step4.setIsLastNode(true);
        startStep(step4);

        // 5. Download both files and verify their contents match the upload
        byte[] zipBytes = given()
            .when().get("/volume/workflow/{workflowId}/node/{workflowNodeId}/download",
                WORKFLOW_ID,
                workflowNodeId)
            .then()
            .statusCode(200)
            .contentType("application/zip")
            .extract().asByteArray();

        Map<String, byte[]> downloadedFiles = new HashMap<>();
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    // shallow iteration is enough
                    downloadedFiles.put(entry.getName(), zis.readAllBytes());
                }
            }
        }

        Assertions.assertEquals(2, downloadedFiles.size(), "Both processed files should be present");
        Assertions.assertArrayEquals(
            Files.readAllBytes(file1),
            downloadedFiles.get("processed_processed_test_1.txt"),
            "Processed file 1 must match the original file 1"
        );
        Assertions.assertArrayEquals(
            Files.readAllBytes(file2),
            downloadedFiles.get("processed_processed_test_2.txt"),
            "Processed file 2 must match the original file 2"
        );

        // 6. Stop the workflow (hard cleanup of all step containers & volume)
        given()
                .when().delete("/docker/workflow/{workflowId}/cleanup", WORKFLOW_ID)
                .then()
                .statusCode(anyOf(is(200), is(204)));

        // 7. Verify no containers of this workflow are left
        given()
                .when().get("/container/running")
                .then()
                .statusCode(200)
                .body("findAll { it.Labels['featurecloud.workflow.id'] == '" + WORKFLOW_ID + "' }.size()", equalTo(0));
    }

    // Start a dumnmy workflow step with needsInternetAccess=false
    // Exec into it, run wget to google.com, and verify it fails!
    // Timeout is set to 5 seconds, so the test should not take too long
    @Test
    public void cannotReachInternetWithoutInternetAccess() throws Exception {
        // Start the container
        long workflowId = 999L;
        StartWorkflowNodeDTO step = getStartDTO(1L);
        step.setWorkflowId(workflowId);
        step.setIsFistNode(true);
        step.setNeedsInternetAccess(false);
        step.setNeedsHostAccess(false);
        step.setNeedsFederatedLearningAccess(false);

        String containerId = null;
        try {
            CreateContainerResponse response = given()
                .contentType(ContentType.JSON)
                .body(step)
                .when().post("/container/workflow")
                .then()
                .statusCode(200)
                .body("Id", notNullValue())
                .extract().as(CreateContainerResponse.class);
            containerId = response.getId();

            // Exec into the container and run wget to google.com, expecting it to fail
            var exec = dockerClient.execCreateCmd(containerId)
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withCmd("wget", "-T", "5", "-q", "-O", "-", "https://google.com")
                .exec();
            dockerClient.execStartCmd(exec.getId())
                .exec(new ResultCallback.Adapter<Frame>())
                .awaitCompletion();

            // Verify the exit code is non-zero (wget should fail)
            Long exitCode = dockerClient.inspectExecCmd(exec.getId()).exec().getExitCodeLong();
            Assertions.assertNotNull(exitCode, "Docker should report the wget exit code");
            Assertions.assertNotEquals(0L, exitCode,
                "wget should fail because the container has no internet access");
        } finally {
            given()
                .when().delete("/docker/workflow/{workflowId}/cleanup", workflowId)
                .then()
                .statusCode(anyOf(is(200), is(204)));
        }
    }


    // Flower SuperLink support: a published port must actually be reachable from the host,
    // independent of the dedicated-network/internet-access wiring applied afterwards.
    @Test
    public void publishedPortIsReachableFromHost() throws Exception {
        int hostPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            hostPort = socket.getLocalPort();
        }

        long workflowId = 998L;
        StartWorkflowNodeDTO step = getStartDTO(1L);
        step.setWorkflowId(workflowId);
        step.setIsFistNode(true);
        step.setPublishContainerPort(80);
        step.setPublishHostPort(hostPort);
        // Docker never wires up a published port on an "internal" network (confirmed separately -
        // this is a different, orthogonal limitation from the one this test targets), so this needs
        // internet access on for now to isolate the create-before-reconnect timing fix.
        step.setNeedsInternetAccess(true);

        try {
            given()
                    .contentType(ContentType.JSON)
                    .body(step)
                    .when().post("/container/workflow")
                    .then()
                    .statusCode(200)
                    .body("Id", notNullValue());

            assertPortBecomesReachable(hostPort);
        } finally {
            given()
                    .when().delete("/docker/workflow/{workflowId}/cleanup", workflowId)
                    .then()
                    .statusCode(anyOf(is(200), is(204)));
        }
    }

    // Docker's "internal" network flag can never coexist with a published port, and there's no other
    // egress-restriction mechanism in orch-api today, so a published port now implies
    // unrestricted outbound access instead of the request simply failing or the port silently
    // never working.
    @Test
    public void publishedPortWorksEvenWhenInternetAccessWasDeclined() throws Exception {
        int hostPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            hostPort = socket.getLocalPort();
        }

        long workflowId = 996L;
        StartWorkflowNodeDTO step = getStartDTO(1L);
        step.setWorkflowId(workflowId);
        step.setIsFistNode(true);
        step.setPublishContainerPort(80);
        step.setPublishHostPort(hostPort);
        step.setNeedsInternetAccess(false);

        try {
            given()
                    .contentType(ContentType.JSON)
                    .body(step)
                    .when().post("/container/workflow")
                    .then()
                    .statusCode(200)
                    .body("Id", notNullValue());

            assertPortBecomesReachable(hostPort);
        } finally {
            given()
                    .when().delete("/docker/workflow/{workflowId}/cleanup", workflowId)
                    .then()
                    .statusCode(anyOf(is(200), is(204)));
        }
    }

    private void assertPortBecomesReachable(int hostPort) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + hostPort + "/"))
                .timeout(Duration.ofSeconds(5))
                .build();

        HttpResponse<String> response = null;
        Exception lastFailure = null;
        for (int attempt = 0; attempt < 10 && response == null; attempt++) {
            try {
                response = client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                lastFailure = e;
                Thread.sleep(500);
            }
        }

        Assertions.assertNotNull(response,
                "Published port " + hostPort + " should become reachable from the host: " + lastFailure);
        Assertions.assertEquals(200, response.statusCode(), "nginx should answer on the published host port");
    }

    // Flower SuperLink support: cert/key material staged into a named volume ahead of time
    // (the existing /volume upload endpoints) must be readable, but not writable, from inside
    // the container it's mounted into.
    @Test
    public void certsVolumeIsMountedReadOnlyInContainer() throws Exception {
        String certsVolumeName = "test-certs-volume-" + System.currentTimeMillis();
        given()
                .contentType(ContentType.JSON)
                .when().post("/volume/{name}", certsVolumeName)
                .then()
                .statusCode(201);

        Path certFile = Files.createTempFile("ca", ".crt");
        Files.writeString(certFile, "dummy-ca-cert-contents");

        try {
            given()
                    .multiPart("file", certFile.toFile())
                    .multiPart("fileName", "ca.crt")
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .when().post("/volume/{name}/upload", certsVolumeName)
                    .then()
                    .statusCode(anyOf(is(200), is(204)));

            long workflowId = 997L;
            StartWorkflowNodeDTO step = getStartDTO(1L);
            step.setWorkflowId(workflowId);
            step.setIsFistNode(true);
            step.setCertsVolumeName(certsVolumeName);

            try {
                CreateContainerResponse response = given()
                        .contentType(ContentType.JSON)
                        .body(step)
                        .when().post("/container/workflow")
                        .then()
                        .statusCode(200)
                        .body("Id", notNullValue())
                        .extract().as(CreateContainerResponse.class);

                String content = execAndCapture(response.getId(), "cat", "/mnt/certs/ca.crt");
                Assertions.assertEquals("dummy-ca-cert-contents", content.trim(),
                        "Certs volume content should be readable inside the container");

                Long writeExitCode = execAndGetExitCode(response.getId(), "sh", "-c", "touch /mnt/certs/should-fail");
                Assertions.assertNotEquals(0L, writeExitCode, "Certs volume should be mounted read-only");
            } finally {
                given()
                        .when().delete("/docker/workflow/{workflowId}/cleanup", workflowId)
                        .then()
                        .statusCode(anyOf(is(200), is(204)));
            }
        } finally {
            Files.deleteIfExists(certFile);
            given()
                    .when().delete("/volume/{name}", certsVolumeName)
                    .then()
                    .statusCode(anyOf(is(200), is(204), is(404)));
        }
    }

    private String execAndCapture(String containerId, String... cmd) throws Exception {
        var exec = dockerClient.execCreateCmd(containerId)
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withCmd(cmd)
                .exec();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        dockerClient.execStartCmd(exec.getId())
                .exec(new ResultCallback.Adapter<Frame>() {
                    @Override
                    public void onNext(Frame frame) {
                        try {
                            out.write(frame.getPayload());
                        } catch (IOException ignored) {
                            // best-effort capture only
                        }
                    }
                })
                .awaitCompletion();
        return out.toString(StandardCharsets.UTF_8);
    }

    private Long execAndGetExitCode(String containerId, String... cmd) throws Exception {
        var exec = dockerClient.execCreateCmd(containerId)
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withCmd(cmd)
                .exec();
        dockerClient.execStartCmd(exec.getId())
                .exec(new ResultCallback.Adapter<Frame>())
                .awaitCompletion();
        return dockerClient.inspectExecCmd(exec.getId()).exec().getExitCodeLong();
    }

    private StartWorkflowNodeDTO getStartDTO(Long workflowNodeId) {
        StartWorkflowNodeDTO step = new StartWorkflowNodeDTO();
        step.setAppImage(TEST_IMAGE);
        step.setWorkflowNodeId(workflowNodeId);
        step.setWorkflowId(WORKFLOW_ID);
        return step;
    }

    private void startStep(StartWorkflowNodeDTO step) {
        given()
                .contentType(ContentType.JSON)
                .body(step)
                .when().post("/container/workflow")
                .then()
                .statusCode(200)
                .body("Id", notNullValue());

        given()
                .contentType(MediaType.APPLICATION_JSON)
                .when().put("/volume/workflow/{workflowId}/node/{workflowNodeId}/move-to-output",
                        WORKFLOW_ID,
                        step.getWorkflowNodeId())
                .then()
                .statusCode(anyOf(is(200), is(204)));
    }

    private List<WorkflowNodeInputActionDTO> getActionDTOs(Long workflowNodeId, boolean isLastUsage, String originalFileName) {
        List<WorkflowNodeInputActionDTO> steps = new ArrayList<>();
        WorkflowNodeInputActionDTO action = new WorkflowNodeInputActionDTO();
        action.setOriginalFileName(originalFileName);
        action.setNewFileName("processed_" + originalFileName);
        action.setInputNodeId(workflowNodeId);
        action.setLastUsage(isLastUsage);
        steps.add(action);
        return steps;
    }
}
