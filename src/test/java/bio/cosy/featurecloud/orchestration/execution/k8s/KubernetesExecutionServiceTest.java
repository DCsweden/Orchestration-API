package bio.cosy.featurecloud.orchestration.execution.k8s;

import bio.cosy.featurecloud.orchestration.config.ContainerConfig;
import bio.cosy.featurecloud.orchestration.docker.ContainerNetworkAccess;
import bio.cosy.featurecloud.orchestration.docker.DockerLabels;
import bio.cosy.featurecloud.orchestration.helper.NamingService;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parts of the Kubernetes backend that decide what a run may reach and
 * what cleanup removes, against fabric8's in-memory API server. Plain JUnit,
 * no Quarkus or cluster needed.
 */
@EnableKubernetesMockClient(crud = true)
class KubernetesExecutionServiceTest {

    private static final String NS = "clinic";
    private static final String SYSTEM = "clinic1";

    KubernetesClient client;

    private KubernetesExecutionService service;

    @BeforeEach
    void setUp() throws Exception {
        setSystemName(SYSTEM);
        service = new KubernetesExecutionService();
        service.client = client;
        service.configuredNamespace = Optional.of(NS);
        service.networkIsolationEnabled = true;
        service.containerConfig = containerConfig("local-learning-api:8080", "controller:8001");

        client.services().inNamespace(NS).resource(new ServiceBuilder()
                .withNewMetadata().withName("local-learning-api").endMetadata()
                .withNewSpec()
                    .withSelector(Map.of("app.kubernetes.io/name", "local-learning-api"))
                    .addNewPort().withPort(8080).withTargetPort(new IntOrString("http")).endPort()
                .endSpec().build()).create();
        client.services().inNamespace(NS).resource(new ServiceBuilder()
                .withNewMetadata().withName("controller").endMetadata()
                .withNewSpec()
                    .withSelector(Map.of("app.kubernetes.io/name", "controller"))
                    .addNewPort().withPort(8000).endPort()
                    .addNewPort().withPort(8001).withTargetPort(new IntOrString(8001)).endPort()
                .endSpec().build()).create();
    }

    @Test
    void peerComesFromTheConfiguredServiceNotFromHardcodedLabels() {
        var peer = service.resolvePeer("local-learning-api:8080", "container.network.learning");
        assertEquals(Map.of("app.kubernetes.io/name", "local-learning-api"), peer.podSelector());
        // The Service's target port, which is what the Pod actually listens on.
        assertEquals(new IntOrString("http"), peer.port());
        assertTrue(peer.sameNamespace());
    }

    @Test
    void peerInAnotherNamespaceIsSelectedByNamespaceToo() {
        client.services().inNamespace("shared").resource(new ServiceBuilder()
                .withNewMetadata().withName("controller").endMetadata()
                .withNewSpec().withSelector(Map.of("app", "ctl")).addNewPort().withPort(8001).endPort().endSpec()
                .build()).create();
        var peer = service.resolvePeer("controller.shared.svc.cluster.local:8001", "container.network.controller");
        assertFalse(peer.sameNamespace());
        assertEquals("shared", peer.asPolicyPeer().getNamespaceSelector().getMatchLabels().get("kubernetes.io/metadata.name"));
    }

    @Test
    void misconfiguredPeersFailBeforeAnythingIsCreated() {
        assertThrows(IllegalStateException.class,
                () -> service.resolvePeer("no-such-service:8080", "container.network.learning"));
        assertThrows(IllegalStateException.class,
                () -> service.resolvePeer("local-learning-api:9999", "container.network.learning"));

        service.containerConfig = containerConfig("local-learning-api:8080", null);
        // Learning API alone is fine; federated learning without a controller is not.
        service.ensureNetworkAccessConfigured(ContainerNetworkAccess.of(false, false, false));
        assertThrows(IllegalStateException.class,
                () -> service.ensureNetworkAccessConfigured(ContainerNetworkAccess.of(false, false, true)));
    }

    @Test
    void cleanupRemovesTheRunItsPolicyAndItsVolumes() {
        createRun("run-1", SYSTEM, "run-1-volume-input", "run-1-volume-output");

        service.cleanupContainer("run-1", true, true, true);

        assertNull(client.batch().v1().jobs().inNamespace(NS).withName("run-1").get());
        assertNull(client.network().v1().networkPolicies().inNamespace(NS).withName("run-1").get());
        assertNull(client.persistentVolumeClaims().inNamespace(NS).withName("run-1-volume-input").get());
        assertNull(client.persistentVolumeClaims().inNamespace(NS).withName("run-1-volume-output").get());
    }

    @Test
    void softCleanupOnlyStopsTheRun() {
        createRun("run-2", SYSTEM, "run-2-volume-output");

        service.cleanupContainer("run-2", false, false, false);

        Job job = client.batch().v1().jobs().inNamespace(NS).withName("run-2").get();
        assertNotNull(job);
        assertTrue(job.getSpec().getSuspend());
        assertNotNull(client.persistentVolumeClaims().inNamespace(NS).withName("run-2-volume-output").get());
    }

    @Test
    void runsOfAnotherDeploymentAreLeftAlone() {
        createRun("run-3", "someone-else", "run-3-volume-output");

        service.cleanupContainer("run-3", true, true, true);

        assertNotNull(client.batch().v1().jobs().inNamespace(NS).withName("run-3").get());
        assertNotNull(client.persistentVolumeClaims().inNamespace(NS).withName("run-3-volume-output").get());
    }

    private void createRun(String name, String system, String... claims) {
        var spec = new JobBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(Map.of(DockerLabels.SYSTEM_NAME.toString(), system))
                .endMetadata()
                .withNewSpec().withNewTemplate().withNewSpec()
                    .addNewContainer().withName("tool").withImage("tool:1").endContainer();
        for (String claim : claims) {
            spec = spec.addNewVolume().withName(claim)
                    .withNewPersistentVolumeClaim().withClaimName(claim).endPersistentVolumeClaim()
                    .endVolume();
            client.persistentVolumeClaims().inNamespace(NS).resource(new PersistentVolumeClaimBuilder()
                    .withNewMetadata().withName(claim).endMetadata().build()).create();
        }
        client.batch().v1().jobs().inNamespace(NS).resource(spec.endSpec().endTemplate().endSpec().build()).create();
        client.network().v1().networkPolicies().inNamespace(NS).resource(new NetworkPolicyBuilder()
                .withNewMetadata().withName(name).endMetadata().build()).create();
    }

    private static void setSystemName(String name) throws Exception {
        Field field = NamingService.class.getDeclaredField("systemName");
        field.setAccessible(true);
        field.set(null, name);
    }

    /** A ContainerConfig with only the network settings these tests read. */
    private static ContainerConfig containerConfig(String learning, String controller) {
        ContainerConfig.Network network = (ContainerConfig.Network) Proxy.newProxyInstance(
                ContainerConfig.class.getClassLoader(), new Class<?>[]{ContainerConfig.Network.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "learning" -> Optional.ofNullable(learning);
                    case "controller" -> Optional.ofNullable(controller);
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (ContainerConfig) Proxy.newProxyInstance(
                ContainerConfig.class.getClassLoader(), new Class<?>[]{ContainerConfig.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("network")) {
                        return network;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
