package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.ranger.plugin.model.RangerRole;
import org.junit.jupiter.api.Test;

public class RangerRoleUtilsTest {

    @Test
    public void testGenerateOwnerRoleName() {
        String rolePrefix = "rolePrefix";
        String expectedOwnerRoleName = "rolePrefix_owner";

        String actualOwnerRoleName = RangerRoleUtils.generateOwnerRoleName(rolePrefix);

        assertEquals(expectedOwnerRoleName, actualOwnerRoleName);
    }

    @Test
    public void testGenerateUserRoleName() {
        String rolePrefix = "rolePrefix";
        String expectedUserRoleName = "rolePrefix_read";

        String actualUserRoleName = RangerRoleUtils.generateUserRoleName(rolePrefix);

        assertEquals(expectedUserRoleName, actualUserRoleName);
    }

    @Test
    public void testNewRangerRole() {
        String roleName = "role name";
        List<String> users = List.of("user1");
        List<String> groups = List.of("group1");
        String deployUser = "deployUser";
        var expectedUsers =
                List.of(new RangerRole.RoleMember("user1", false), new RangerRole.RoleMember("deployUser", true));
        var expectedGroups = List.of(new RangerRole.RoleMember("group1", false));

        var actualRes = RangerRoleUtils.rangerRole(roleName, users, groups, deployUser);

        assertEquals(0L, actualRes.getId());
        assertEquals(true, actualRes.getIsEnabled());
        assertEquals("role_name", actualRes.getName());
        assertEquals("", actualRes.getDescription());
        assertEquals(expectedUsers, actualRes.getUsers());
        assertEquals(expectedGroups, actualRes.getGroups());
    }

    @Test
    public void testExistingRangerRole() {
        var existingRangerRole = new RangerRole();
        existingRangerRole.setId(1L);
        existingRangerRole.setIsEnabled(true);
        existingRangerRole.setName("role_name");
        existingRangerRole.setDescription("");
        existingRangerRole.setUsers(
                List.of(new RangerRole.RoleMember("user1", false), new RangerRole.RoleMember("deployUser1", true)));
        existingRangerRole.setGroups(List.of(new RangerRole.RoleMember("group1", false)));
        var expectedUsers =
                List.of(new RangerRole.RoleMember("user2", false), new RangerRole.RoleMember("deployUser1", true));
        var expectedGroups = List.of(new RangerRole.RoleMember("group2", false));

        var actualRes = RangerRoleUtils.rangerRole(existingRangerRole, List.of("user2"), List.of("group2"));

        assertEquals(1L, actualRes.getId());
        assertEquals(true, actualRes.getIsEnabled());
        assertEquals("role_name", actualRes.getName());
        assertEquals("", actualRes.getDescription());
        assertEquals(expectedUsers, actualRes.getUsers());
        assertEquals(expectedGroups, actualRes.getGroups());
    }
}
