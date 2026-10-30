package bio.cosy.featurecloud.orchestration.execution.k8s;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Kubernetes label values are far more restricted than Docker's (must match
 * {@code (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?}, max 63 chars) — this
 * sanitizes {@code LabelsHelper}'s output (built for Docker, where almost
 * any string was fine) before it's attached to a Job/Pod. The label *keys*
 * from {@code LabelsHelper} (e.g. {@code feature-cloud.workflow-id}) are
 * already K8s-safe as-is.
 */
final class K8sLabels {

    private static final Pattern UNSAFE_CHARS = Pattern.compile("[^A-Za-z0-9._-]");

    private K8sLabels() {
    }

    static Map<String, String> sanitize(Map<String, String> dockerLabels) {
        Map<String, String> sanitized = new LinkedHashMap<>();
        dockerLabels.forEach((key, value) -> sanitized.put(key, sanitizeValue(value)));
        return sanitized;
    }

    static String sanitizeValue(String value) {
        if (value == null || value.isBlank()) {
            return "unset";
        }
        String cleaned = UNSAFE_CHARS.matcher(value).replaceAll("-");
        if (cleaned.length() > 63) {
            cleaned = cleaned.substring(0, 63);
        }
        // trim leading/trailing non-alphanumeric left over from truncation/replacement
        cleaned = cleaned.replaceAll("^[^A-Za-z0-9]+", "").replaceAll("[^A-Za-z0-9]+$", "");
        return cleaned.isEmpty() ? "unset" : cleaned;
    }
}
