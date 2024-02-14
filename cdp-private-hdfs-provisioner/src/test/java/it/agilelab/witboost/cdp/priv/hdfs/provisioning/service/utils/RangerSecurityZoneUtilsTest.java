package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.junit.jupiter.api.Test;

public class RangerSecurityZoneUtilsTest {

    @Test
    public void testNewSecurityZone() {
        String zoneName = "zone name";
        String serviceName = "serviceName";
        String deployUser = "deployUser";
        String path = "path";
        var expectedRes = new RangerSecurityZone();
        expectedRes.setId(-1L);
        expectedRes.setName("zone_name");
        expectedRes.setAdminUsers(List.of(deployUser));
        expectedRes.setAuditUsers(List.of(deployUser));
        expectedRes.setServices(Map.of(
                "serviceName",
                new RangerSecurityZone.RangerSecurityZoneService(
                        List.of((new HashMap<>(Map.of("path", List.of(path))))))));

        var actualRes = RangerSecurityZoneUtils.securityZone(zoneName, serviceName, deployUser, path);

        assertEquals(expectedRes.getId(), actualRes.getId());
        assertEquals(expectedRes.getName(), actualRes.getName());
        assertEquals(expectedRes.getAdminUsers(), actualRes.getAdminUsers());
        assertEquals(expectedRes.getAuditUsers(), actualRes.getAuditUsers());
        expectedRes.getServices().forEach((k, v) -> {
            assertEquals(v.getResources(), actualRes.getServices().get(k).getResources());
        });
    }

    @Test
    public void testExistingSecurityZone() {
        String serviceName = "serviceName";
        String deployUser = "deployUser";
        String path1 = "path1";
        String path2 = "path2";
        var existingSZ = new RangerSecurityZone();
        existingSZ.setId(-1L);
        existingSZ.setName("zone_name");
        existingSZ.setAdminUsers(List.of(deployUser));
        existingSZ.setAuditUsers(List.of(deployUser));
        existingSZ.setServices(Map.of(
                "serviceName",
                new RangerSecurityZone.RangerSecurityZoneService(
                        List.of((new HashMap<>(Map.of("path", List.of(path1))))))));

        var actualRes = RangerSecurityZoneUtils.securityZone(existingSZ, serviceName, path2);

        assertEquals(existingSZ.getId(), actualRes.getId());
        assertEquals(existingSZ.getName(), actualRes.getName());
        assertEquals(existingSZ.getAdminUsers(), actualRes.getAdminUsers());
        assertEquals(existingSZ.getAuditUsers(), actualRes.getAuditUsers());
        actualRes.getServices().forEach((k, v) -> {
            if (k.equals(serviceName)) assertEquals(v.getResources(), List.of(Map.of("path", List.of(path1, path2))));
        });
    }
}
