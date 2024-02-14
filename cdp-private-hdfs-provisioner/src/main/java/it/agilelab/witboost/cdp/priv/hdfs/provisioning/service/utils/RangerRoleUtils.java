package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ranger.plugin.model.RangerRole;

public class RangerRoleUtils {

    private static final String OWNER_ROLE_NAME_PATTERN = "%s_owner";
    private static final String USER_ROLE_NAME_PATTERN = "%s_read";

    public static String generateOwnerRoleName(String rolePrefix) {
        return clean(String.format(OWNER_ROLE_NAME_PATTERN, rolePrefix));
    }

    public static String generateUserRoleName(String rolePrefix) {
        return clean(String.format(USER_ROLE_NAME_PATTERN, rolePrefix));
    }

    public static RangerRole rangerRole(String roleName, List<String> users, List<String> groups, String deployUser) {
        RangerRole rangerRole = new RangerRole();
        rangerRole.setId(0L);
        rangerRole.setIsEnabled(true);
        rangerRole.setName(clean(roleName));
        rangerRole.setDescription("");
        rangerRole.setGroups(
                groups.stream().map(g -> new RangerRole.RoleMember(g, false)).collect(Collectors.toList()));
        rangerRole.setUsers(Stream.concat(
                        users.stream().map(u -> new RangerRole.RoleMember(u, false)),
                        Stream.of(deployUser).map(ou -> new RangerRole.RoleMember(ou, true)))
                .collect(Collectors.toList()));
        return rangerRole;
    }

    public static RangerRole rangerRole(RangerRole existingRole, List<String> users, List<String> groups) {
        existingRole.setUsers(Stream.concat(
                        users.stream().map(u -> new RangerRole.RoleMember(u, false)),
                        existingRole.getUsers().stream().filter(RangerRole.RoleMember::getIsAdmin))
                .distinct()
                .toList());
        existingRole.setGroups(Stream.concat(
                        groups.stream().map(g -> new RangerRole.RoleMember(g, false)),
                        existingRole.getGroups().stream().filter(RangerRole.RoleMember::getIsAdmin))
                .distinct()
                .toList());
        return existingRole;
    }

    private static String clean(String n) {
        return n.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
