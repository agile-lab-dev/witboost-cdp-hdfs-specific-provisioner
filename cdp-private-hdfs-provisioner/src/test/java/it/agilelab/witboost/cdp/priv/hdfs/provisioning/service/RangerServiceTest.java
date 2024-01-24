package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import static org.apache.ranger.RangerClient.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import com.sun.jersey.api.client.ClientResponse;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RangerServiceTest {
    @Mock private RangerClient rangerClient;

    @InjectMocks private RangerServiceImpl rangerService;

    private final String securityZoneName = "my_sz_name";
    private final long securityZoneId = 1;
    private final RangerSecurityZone rangerSecurityZone = new RangerSecurityZone();
    private final String serviceName = "my_service_name";
    private final String policyName = "my_policy_name";
    private final long policyId = 1;
    private final RangerPolicy rangerPolicy = new RangerPolicy();
    private final String roleName = "my_role_name";
    private final long roleId = 1;
    private final RangerRole rangerRole = new RangerRole();

    public RangerServiceTest() {
        rangerSecurityZone.setName(securityZoneName);
        rangerSecurityZone.setId(securityZoneId);

        rangerPolicy.setName(policyName);
        rangerPolicy.setService(serviceName);
        rangerPolicy.setZoneName(securityZoneName);
        rangerPolicy.setId(policyId);

        rangerRole.setName(roleName);
        rangerRole.setId(roleId);
    }

    @Test
    public void testFindSecurityZoneByNameWithExistingSZ() throws RangerServiceException {
        when(rangerClient.getSecurityZone(securityZoneName)).thenReturn(rangerSecurityZone);

        var actualRes = rangerService.findSecurityZoneByName(securityZoneName);

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isPresent());
        assertEquals(rangerSecurityZone, actualRes.get().get());
    }

    @Test
    public void testFindSecurityZoneByNameWithNotExistingSZ() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus()).thenReturn(ClientResponse.Status.NOT_FOUND.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(GET_ZONE_BY_NAME, response);
        when(rangerClient.getSecurityZone(securityZoneName)).thenThrow(ex);

        var actualRes = rangerService.findSecurityZoneByName(securityZoneName);

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isEmpty());
    }

    @Test
    public void testFindSecurityZoneByNameWithInternalError() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(GET_ZONE_BY_NAME, response);
        when(rangerClient.getSecurityZone(securityZoneName)).thenThrow(ex);
        var expectedDesc =
                "An error occurred while searching for security zone 'my_sz_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = rangerService.findSecurityZoneByName(securityZoneName);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }

    @Test
    public void testCreateSecurityZoneWithSuccess() throws RangerServiceException {
        when(rangerClient.createSecurityZone(rangerSecurityZone)).thenReturn(rangerSecurityZone);

        var actualRes = rangerService.createSecurityZone(rangerSecurityZone);

        assertTrue(actualRes.isRight());
        assertEquals(rangerSecurityZone, actualRes.get());
    }

    @Test
    public void testCreateSecurityZoneWithError() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(GET_ZONE_BY_NAME, response);
        when(rangerClient.createSecurityZone(rangerSecurityZone)).thenThrow(ex);
        var expectedDesc =
                "An error occurred while creating the security zone 'my_sz_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = rangerService.createSecurityZone(rangerSecurityZone);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }

    @Test
    public void testUpdateSecurityZoneWithSuccess() throws RangerServiceException {
        when(rangerClient.updateSecurityZone(securityZoneId, rangerSecurityZone))
                .thenReturn(rangerSecurityZone);

        var actualRes = rangerService.updateSecurityZone(rangerSecurityZone);

        assertTrue(actualRes.isRight());
        assertEquals(rangerSecurityZone, actualRes.get());
    }

    @Test
    public void testUpdateSecurityZoneWithError() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(GET_ZONE_BY_NAME, response);
        when(rangerClient.updateSecurityZone(securityZoneId, rangerSecurityZone)).thenThrow(ex);
        var expectedDesc =
                "An error occurred while updating the security zone 'my_sz_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = rangerService.updateSecurityZone(rangerSecurityZone);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }

    @Test
    public void testFindPolicyByNameWithExistingPolicy() throws RangerServiceException {
        var filter =
                Map.of("serviceName", serviceName, "policyName", policyName, "zoneName", securityZoneName);
        when(rangerClient.findPolicies(filter)).thenReturn(Collections.singletonList(rangerPolicy));

        var actualRes =
                rangerService.findPolicyByName(serviceName, policyName, Optional.of(securityZoneName));

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isPresent());
        assertEquals(rangerPolicy, actualRes.get().get());
    }

    @Test
    public void testFindPolicyByNameWithExistingPolicyWithoutZone() throws RangerServiceException {
        var filter = Map.of("serviceName", serviceName, "policyName", policyName);
        when(rangerClient.findPolicies(filter)).thenReturn(Collections.singletonList(rangerPolicy));

        var actualRes = rangerService.findPolicyByName(serviceName, policyName, Optional.empty());

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isPresent());
        assertEquals(rangerPolicy, actualRes.get().get());
    }

    @Test
    public void testFindPolicyByNameWithNotExistingPolicy() throws RangerServiceException {
        var filter =
                Map.of("serviceName", serviceName, "policyName", policyName, "zoneName", securityZoneName);
        when(rangerClient.findPolicies(filter)).thenReturn(Collections.emptyList());

        var actualRes =
                rangerService.findPolicyByName(serviceName, policyName, Optional.of(securityZoneName));

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isEmpty());
    }

    @Test
    public void testFindPolicyByNameWithInternalError() throws RangerServiceException {
        var filter =
                Map.of("serviceName", serviceName, "policyName", policyName, "zoneName", securityZoneName);
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(FIND_POLICIES, response);
        when(rangerClient.findPolicies(filter)).thenThrow(ex);
        var expectedDesc =
                "An error occurred while searching for policy 'my_policy_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes =
                rangerService.findPolicyByName(serviceName, policyName, Optional.of(securityZoneName));

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }

    @Test
    public void testCreatePolicyWithSuccess() throws RangerServiceException {
        when(rangerClient.createPolicy(rangerPolicy)).thenReturn(rangerPolicy);

        var actualRes = rangerService.createPolicy(rangerPolicy);

        assertTrue(actualRes.isRight());
        assertEquals(rangerPolicy, actualRes.get());
    }

    @Test
    public void testCreatePolicyWithError() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(CREATE_POLICY, response);
        when(rangerClient.createPolicy(rangerPolicy)).thenThrow(ex);
        var expectedDesc =
                "An error occurred while creating the policy 'my_policy_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = rangerService.createPolicy(rangerPolicy);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }

    @Test
    public void testUpdatePolicyWithSuccess() throws RangerServiceException {
        when(rangerClient.updatePolicy(rangerPolicy.getId(), rangerPolicy)).thenReturn(rangerPolicy);

        var actualRes = rangerService.updatePolicy(rangerPolicy);

        assertTrue(actualRes.isRight());
        assertEquals(rangerPolicy, actualRes.get());
    }

    @Test
    public void testUpdatePolicyWithError() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(UPDATE_POLICY_BY_NAME, response);
        when(rangerClient.updatePolicy(rangerPolicy.getId(), rangerPolicy)).thenThrow(ex);
        var expectedDesc =
                "An error occurred while updating the policy 'my_policy_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = rangerService.updatePolicy(rangerPolicy);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }

    @Test
    public void testDeletePolicyWithSuccess() throws RangerServiceException {
        doNothing().when(rangerClient).deletePolicy(policyId);

        var actualRes = rangerService.deletePolicy(rangerPolicy);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testDeletePolicyWithError() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(DELETE_POLICY_BY_NAME, response);
        doThrow(ex).when(rangerClient).deletePolicy(policyId);
        var expectedDesc =
                "An error occurred while deleting the policy 'my_policy_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = rangerService.deletePolicy(rangerPolicy);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }

    @Test
    public void testFindRoleByNameWithExistingRole() throws RangerServiceException {
        when(rangerClient.findRoles(Collections.singletonMap("roleName", roleName)))
                .thenReturn(Collections.singletonList(rangerRole));

        var actualRes = rangerService.findRoleByName(roleName);

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isPresent());
        assertEquals(rangerRole, actualRes.get().get());
    }

    @Test
    public void testFindRoleByNameWithNotExistingRole() throws RangerServiceException {
        when(rangerClient.findRoles(Collections.singletonMap("roleName", roleName)))
                .thenReturn(Collections.emptyList());

        var actualRes = rangerService.findRoleByName(roleName);

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isEmpty());
    }

    @Test
    public void testFindRoleByNameWithInternalError() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(FIND_ROLES, response);
        when(rangerClient.findRoles(Collections.singletonMap("roleName", roleName))).thenThrow(ex);
        var expectedDesc =
                "An error occurred while searching for role 'my_role_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = rangerService.findRoleByName(roleName);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }

    @Test
    public void testCreateRoleWithSuccess() throws RangerServiceException {
        when(rangerClient.createRole("", rangerRole)).thenReturn(rangerRole);

        var actualRes = rangerService.createRole(rangerRole);

        assertTrue(actualRes.isRight());
        assertEquals(rangerRole, actualRes.get());
    }

    @Test
    public void testCreateRoleWithError() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(CREATE_ROLE, response);
        when(rangerClient.createRole("", rangerRole)).thenThrow(ex);
        var expectedDesc =
                "An error occurred while creating the role 'my_role_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = rangerService.createRole(rangerRole);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }

    @Test
    public void testUpdateRoleWithSuccess() throws RangerServiceException {
        when(rangerClient.updateRole(rangerRole.getId(), rangerRole)).thenReturn(rangerRole);

        var actualRes = rangerService.updateRole(rangerRole);

        assertTrue(actualRes.isRight());
        assertEquals(rangerRole, actualRes.get());
    }

    @Test
    public void testUpdateRoleWithError() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(UPDATE_ROLE_BY_ID, response);
        when(rangerClient.updateRole(rangerRole.getId(), rangerRole)).thenThrow(ex);
        var expectedDesc =
                "An error occurred while updating the role 'my_role_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = rangerService.updateRole(rangerRole);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }

    @Test
    public void testDeleteRoleWithSuccess() throws RangerServiceException {
        doNothing().when(rangerClient).deleteRole(roleId);

        var actualRes = rangerService.deleteRole(rangerRole);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testDeleteRoleWithError() throws RangerServiceException {
        ClientResponse response = mock(ClientResponse.class);
        when(response.getStatus())
                .thenReturn(ClientResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        when(response.getEntity(String.class)).thenReturn("");
        var ex = new RangerServiceException(DELETE_ROLE_BY_ID, response);
        doThrow(ex).when(rangerClient).deleteRole(roleId);
        var expectedDesc =
                "An error occurred while deleting the role 'my_role_name' on Ranger. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = rangerService.deleteRole(rangerRole);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                            assertEquals(ex, p.cause().get());
                        });
    }
}
