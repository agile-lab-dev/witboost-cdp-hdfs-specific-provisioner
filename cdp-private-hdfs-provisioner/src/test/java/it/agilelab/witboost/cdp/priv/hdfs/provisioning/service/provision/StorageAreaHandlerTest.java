package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.RangerConfig;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.*;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.HdfsService;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.PrincipalMappingService;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.RangerService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StorageAreaHandlerTest {

    @Mock
    private PrincipalMappingService principalMappingService;

    @Mock
    private RangerService rangerService;

    @Mock
    private HdfsService hdfsService;

    @Mock
    private RangerConfig rangerConfig;

    @InjectMocks
    private StorageAreaHandler storageAreaHandler;

    private final ProvisionRequest<StorageSpecific> provisionRequest;
    private final ProvisionRequest<StorageSpecific> provisionRequestRemoveData;
    private final ProvisionRequest<StorageSpecific> provisionRequestWrongComponentId;
    private final ProvisionRequest<StorageSpecific> provisionRequestNotSetSpecific;

    public StorageAreaHandlerTest() {
        StorageArea<StorageSpecific> storageArea = new StorageArea<>();
        storageArea.setKind("storage");
        storageArea.setId("urn:dmb:cmp:healthcare:vaccinations:0:storage");
        StorageSpecific storageSpecific = new StorageSpecific();
        storageSpecific.setPrefixPath("prefixPath");
        storageArea.setSpecific(storageSpecific);
        DataProduct dp = new DataProduct();
        dp.setDataProductOwner("ownerUser");
        dp.setDevGroup("ownerGroup");
        provisionRequest = new ProvisionRequest<>(dp, storageArea, false);
        provisionRequestRemoveData = new ProvisionRequest<>(dp, storageArea, true);

        StorageArea<StorageSpecific> storageAreaWrongId = new StorageArea<>();
        storageAreaWrongId.setKind("storage");
        storageAreaWrongId.setId("wrong_id");
        storageAreaWrongId.setSpecific(storageSpecific);
        provisionRequestWrongComponentId = new ProvisionRequest<>(dp, storageAreaWrongId, false);

        StorageArea<StorageSpecific> storageAreaNotSetSpecific = new StorageArea<>();
        storageAreaNotSetSpecific.setKind("storage");
        storageAreaNotSetSpecific.setId("urn:dmb:cmp:healthcare:vaccinations:0:storage");
        provisionRequestNotSetSpecific = new ProvisionRequest<>(dp, storageAreaNotSetSpecific, true);
    }

    @Test
    public void testCreateNotExistingEntitiesOk() {
        when(principalMappingService.map(Set.of("ownerUser", "ownerGroup")))
                .thenReturn(Map.of("ownerUser", right(new CDPUser("", "")), "ownerGroup", right(new CDPGroup(""))));
        when(rangerConfig.hdfsServiceName()).thenReturn("cm_hdfs");
        when(rangerService.findSecurityZoneByName(anyString())).thenReturn(right(Optional.empty()));
        when(rangerService.createSecurityZone(any())).thenReturn(right(new RangerSecurityZone()));
        when(rangerService.findRoleByName(anyString())).thenReturn(right(Optional.empty()));
        when(rangerService.createRole(any())).thenReturn(right(new RangerRole()));
        when(rangerService.findRoleByName(anyString())).thenReturn(right(Optional.empty()));
        when(rangerService.createRole(any())).thenReturn(right(new RangerRole()));
        when(rangerService.findPolicyByName(anyString(), anyString(), any())).thenReturn(right(Optional.empty()));
        when(rangerService.createPolicy(any())).thenReturn(right(new RangerPolicy()));
        when(hdfsService.createFolder(anyString())).thenReturn(right(""));
        when(rangerConfig.ownerTechnicalUser()).thenReturn("admin");

        var actualRes = storageAreaHandler.create(provisionRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testCreateExistingEntitiesOk() {
        when(principalMappingService.map(Set.of("ownerUser", "ownerGroup")))
                .thenReturn(Map.of("ownerUser", right(new CDPUser("", "")), "ownerGroup", right(new CDPGroup(""))));
        when(rangerConfig.hdfsServiceName()).thenReturn("cm_hdfs");
        when(rangerService.findSecurityZoneByName(anyString()))
                .thenReturn(right(Optional.of(new RangerSecurityZone())));
        when(rangerService.updateSecurityZone(any())).thenReturn(right(new RangerSecurityZone()));
        when(rangerService.findRoleByName(anyString())).thenReturn(right(Optional.of(new RangerRole())));
        when(rangerService.updateRole(any())).thenReturn(right(new RangerRole()));
        when(rangerService.findRoleByName(anyString())).thenReturn(right(Optional.of(new RangerRole())));
        when(rangerService.updateRole(any())).thenReturn(right(new RangerRole()));
        when(rangerService.findPolicyByName(anyString(), anyString(), any()))
                .thenReturn(right(Optional.of(new RangerPolicy())));
        when(rangerService.updatePolicy(any())).thenReturn(right(new RangerPolicy()));
        when(hdfsService.createFolder(anyString())).thenReturn(right(""));

        var actualRes = storageAreaHandler.create(provisionRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testDestroyNotExistingEntitiesOk() {
        when(rangerConfig.hdfsServiceName()).thenReturn("cm_hdfs");
        when(rangerService.findPolicyByName(anyString(), anyString(), any())).thenReturn(right(Optional.empty()));
        when(rangerService.findRoleByName(anyString())).thenReturn(right(Optional.empty()));

        var actualRes = storageAreaHandler.destroy(provisionRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testDestroyExistingEntitiesOk() {
        when(rangerConfig.hdfsServiceName()).thenReturn("cm_hdfs");
        when(rangerService.findPolicyByName(anyString(), anyString(), any()))
                .thenReturn(right(Optional.of(new RangerPolicy())));
        when(rangerService.deletePolicy(any())).thenReturn(right(null));
        when(rangerService.findRoleByName(anyString())).thenReturn(right(Optional.of(new RangerRole())));
        when(rangerService.deleteRole(any())).thenReturn(right(null));
        when(hdfsService.deleteFolder(anyString())).thenReturn(right(""));

        var actualRes = storageAreaHandler.destroy(provisionRequestRemoveData);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testCreateWrongComponentId() {
        when(principalMappingService.map(Set.of("ownerUser", "ownerGroup")))
                .thenReturn(Map.of("ownerUser", right(new CDPUser("", "")), "ownerGroup", right(new CDPGroup(""))));
        String expectedError = "Component id 'wrong_id' is not in the expected shape, cannot extract attributes";

        var actualRes = storageAreaHandler.create(provisionRequestWrongComponentId);

        assertTrue(actualRes.isLeft());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedError, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testDestroyWrongComponentId() {
        String expectedError = "Component id 'wrong_id' is not in the expected shape, cannot extract attributes";

        var actualRes = storageAreaHandler.destroy(provisionRequestWrongComponentId);

        assertTrue(actualRes.isLeft());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedError, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testDestroyNotSetSpecificComponent() {
        when(rangerConfig.hdfsServiceName()).thenReturn("cm_hdfs");
        when(rangerService.findPolicyByName(anyString(), anyString(), any())).thenReturn(right(Optional.empty()));
        when(rangerService.findRoleByName(anyString())).thenReturn(right(Optional.empty()));
        String expectedError =
                "The specific section of the component urn:dmb:cmp:healthcare:vaccinations:0:storage is not of type StorageSpecific";

        var actualRes = storageAreaHandler.destroy(provisionRequestNotSetSpecific);

        assertTrue(actualRes.isLeft());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedError, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testUpdateAcl() {
        // When
        var actualRes = storageAreaHandler.updateAcl();

        // Then
        FailedOperation expectedRes = new FailedOperation(
                List.of(new Problem("Updating Access Control Lists is not supported by the Storage Area Component")));
        assertEquals(expectedRes, actualRes);
    }
}
