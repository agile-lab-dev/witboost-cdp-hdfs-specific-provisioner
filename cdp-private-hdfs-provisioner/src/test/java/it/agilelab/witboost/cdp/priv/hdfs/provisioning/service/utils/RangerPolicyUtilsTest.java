package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.Test;

public class RangerPolicyUtilsTest {

    private final String prefixName = "prefix";
    private final String zoneName = "zone";
    private final String folderPath = "mypath";
    private final String ownerRole = "owner";
    private final String userRole = "user";
    private final String hdfsServiceName = "cm_hdfs";
    private final List<RangerPolicy.RangerPolicyItem> expectedRangerPolicyItems = List.of(
            new RangerPolicy.RangerPolicyItem(
                    List.of(
                            new RangerPolicy.RangerPolicyItemAccess("READ", true),
                            new RangerPolicy.RangerPolicyItemAccess("WRITE", true)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    List.of(ownerRole),
                    Collections.emptyList(),
                    false),
            new RangerPolicy.RangerPolicyItem(
                    List.of(new RangerPolicy.RangerPolicyItemAccess("READ", true)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    List.of(userRole),
                    Collections.emptyList(),
                    false));

    @Test
    public void testPolicyName() {
        String policyPrefix = "policy Prefix";
        String expected = "policy_Prefix_access_policy";

        String actual = RangerPolicyUtils.policyName(policyPrefix);

        assertEquals(expected, actual);
    }

    @Test
    public void testNewRangerPolicy() {
        var actualRes =
                RangerPolicyUtils.rangerPolicy(prefixName, zoneName, folderPath, ownerRole, userRole, hdfsServiceName);

        assertEquals(-1L, actualRes.getId());
        assertFields(actualRes);
    }

    private void assertFields(RangerPolicy actualRes) {
        assertEquals("cm_hdfs", actualRes.getService());
        assertEquals("prefix_access_policy", actualRes.getName());
        assertEquals("prefix_access_policy", actualRes.getDescription());
        assertEquals(true, actualRes.getIsAuditEnabled());
        assertEquals(true, actualRes.getIsEnabled());
        assertEquals(
                Map.of("path", new RangerPolicy.RangerPolicyResource(folderPath, false, true)),
                actualRes.getResources());
        assertEquals(expectedRangerPolicyItems, actualRes.getPolicyItems());
        assertEquals("HDFS", actualRes.getServiceType());
        assertEquals(List.of("autogenerated"), actualRes.getPolicyLabels());
        assertEquals(true, actualRes.getIsDenyAllElse());
        assertEquals(zoneName, actualRes.getZoneName());
        assertEquals(RangerPolicy.POLICY_PRIORITY_NORMAL, actualRes.getPolicyPriority());
    }

    @Test
    public void testExistingRangerPolicy() {
        RangerPolicy existingRangerPolicy = new RangerPolicy();
        existingRangerPolicy.setId(2L);

        var actualRes = RangerPolicyUtils.rangerPolicy(
                existingRangerPolicy, prefixName, zoneName, folderPath, ownerRole, userRole, hdfsServiceName);

        assertEquals(2L, actualRes.getId());
        assertFields(actualRes);
    }
}
