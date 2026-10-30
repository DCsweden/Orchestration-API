package bio.cosy.featurecloud.orchestration.api.orchestration.volume;

import java.util.Map;

/**
 * Backend-agnostic replacement for docker-java's {@code InspectVolumeResponse}
 * in the admin {@code /volume/*} REST contract — that type is Docker-specific
 * (driver, mountpoint, UsageData) and can't represent a Kubernetes
 * PersistentVolumeClaim. Both {@code DockerVolumeAdminAdapter} and
 * {@code KubernetesVolumeAdminService} populate this from whichever backend
 * is active.
 */
public class VolumeInfoDTO {

    public String name;
    public String backend; // "docker" or "kubernetes"
    public String status; // e.g. docker driver name, or PVC phase (Bound/Pending)
    public Long sizeBytes; // null when unknown/not cheaply computable
    public Map<String, String> labels;

    public VolumeInfoDTO() {
    }

    public VolumeInfoDTO(String name, String backend, String status, Long sizeBytes, Map<String, String> labels) {
        this.name = name;
        this.backend = backend;
        this.status = status;
        this.sizeBytes = sizeBytes;
        this.labels = labels;
    }
}
