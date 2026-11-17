package bio.cosy.featurecloud.orchestration.api.orchestration.workflow;

import bio.cosy.featurecloud.orchestration.api.orchestration.workflow.actions.WorkflowNodeInputActionDTO;
import bio.cosy.featurecloud.orchestration.helper.BaseName;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class StartWorkflowNodeDTO implements BaseName {

    @NotNull(message = "workflowNodeId cannot be null")
    private Long workflowNodeId;

    @NotBlank(message = "appImage cannot be blank")
    private String appImage;

    @NotNull(message = "workflowId cannot be null")
    private Long workflowId;

    private List<WorkflowNodeInputActionDTO> inputs = new ArrayList<>();

    private Boolean isFistNode = false;
    private Boolean isLastNode = false;

    //Network settings
    private Boolean needsInternetAccess = false;
    private Boolean needsHostAccess = false;
    private Boolean needsFederatedLearningAccess = false;

    private Boolean enableRemoteResultSaving = false;

    // Flower SuperLink support: a volume the caller has already uploaded cert/key material into
    // via the existing /volume endpoints (orch-api never generates or sees the key material
    // itself). Mounted read-only; null means no cert volume is mounted, unchanged for every
    // other tool.
    private String certsVolumeName;

    // Flower SuperLink support: publishes a single container port to the host so clinics'
    // SuperNodes can reach it directly. The host port is chosen by the caller (not auto-assigned
    // by Docker) so no response-shape change is needed to report it back. Both null means no
    // port is published, unchanged for every other tool.
    private Integer publishContainerPort;
    private Integer publishHostPort;

    private List<String> environments = new ArrayList<>();

    public String getBasename() {
        return getWorkflowContainerName(workflowId, workflowNodeId);
    }

    public String getContainerName() {
        String name = getBasename();
        name = name.replaceAll("_", "-");
        return name;
    }

    public String getInputVolumeName() {
        return createInputVolumeName(getContainerName());
    }

    public String getOutputVolumeName() {
        return createOutputVolumeName(getContainerName());
    }


    public void addEnvironment(String env) {
        if (env != null && !env.isBlank()) {
            environments.add(env);
        }
    }
}
