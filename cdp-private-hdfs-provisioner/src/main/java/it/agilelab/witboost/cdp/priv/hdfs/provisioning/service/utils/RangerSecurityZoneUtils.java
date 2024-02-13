package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ranger.plugin.model.RangerSecurityZone;

public class RangerSecurityZoneUtils {

    public static RangerSecurityZone securityZone(
            String zoneName, String serviceName, String deployUser, String path) {
        RangerSecurityZone rangerSecurityZone = new RangerSecurityZone();
        rangerSecurityZone.setId(-1L);
        rangerSecurityZone.setName(clean(zoneName));
        rangerSecurityZone.setAdminUsers(List.of(deployUser));
        rangerSecurityZone.setAuditUsers(List.of(deployUser));
        rangerSecurityZone.setServices(
                Map.of(
                        serviceName,
                        new RangerSecurityZone.RangerSecurityZoneService(
                                List.of((new HashMap<>(Map.of("path", List.of(path))))))));
        return rangerSecurityZone;
    }

    public static RangerSecurityZone securityZone(
            RangerSecurityZone existingRangerSecurityZone, String serviceName, String path) {
        var existingServices = new HashMap<>(existingRangerSecurityZone.getServices());
        existingServices.merge(
                serviceName,
                new RangerSecurityZone.RangerSecurityZoneService(
                        List.of((new HashMap<>(Map.of("path", List.of(path)))))),
                (v1, v2) ->
                        new RangerSecurityZone.RangerSecurityZoneService(
                                List.of((new HashMap<>(Map.of("path", merge(v1, path)))))));
        existingRangerSecurityZone.setServices(existingServices);
        return existingRangerSecurityZone;
    }

    public static String zoneName(String zoneName) {
        return clean(zoneName);
    }

    private static List<String> merge(RangerSecurityZone.RangerSecurityZoneService old, String path) {
        String mapKey = "path";
        return Stream.concat(
                        old.getResources().stream()
                                .filter(r -> r.containsKey(mapKey))
                                .flatMap(m -> m.get(mapKey).stream()),
                        Stream.of(path))
                .distinct()
                .collect(Collectors.toList());
    }

    private static String clean(String n) {
        return n.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
