package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.*;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.RangerService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.ranger.plugin.model.RangerRole;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OutputPortHandlerTest {

    @Mock
    private RangerService rangerService;

    @InjectMocks
    private OutputPortHandler outputPortHandler;

    private final OutputPort<Specific> outputPort;
    private final OutputPort<Specific> outputPortNoDepends;
    private final StorageArea<StorageSpecific> storageArea;
    private final StorageArea<StorageSpecific> storageAreaNoSpecific;

    public OutputPortHandlerTest() {
        Specific specific = new Specific();

        StorageSpecific storageSpecific = new StorageSpecific();
        storageSpecific.setPrefixPath("myprefix");

        outputPort = new OutputPort<>();
        outputPort.setKind("outputport");
        outputPort.setId("urn:dmb:cmp:healthcare:vaccinations:0:outputport");
        outputPort.setDependsOn(List.of("urn:dmb:cmp:healthcare:vaccinations:0:storage"));
        outputPort.setSpecific(specific);

        storageArea = new StorageArea<>();
        storageArea.setId("urn:dmb:cmp:healthcare:vaccinations:0:storage");
        storageArea.setName("storage name");
        storageArea.setDescription("storage desc");
        storageArea.setKind("storage");
        storageArea.setSpecific(storageSpecific);

        outputPortNoDepends = new OutputPort<>();
        outputPortNoDepends.setKind("outputport");
        outputPortNoDepends.setId("urn:dmb:cmp:healthcare:vaccinations:0:outputport2");
        outputPortNoDepends.setDependsOn(Collections.emptyList());
        outputPortNoDepends.setSpecific(specific);

        storageAreaNoSpecific = new StorageArea<>();
        storageAreaNoSpecific.setId("urn:dmb:cmp:healthcare:vaccinations:0:storage");
        storageAreaNoSpecific.setName("storage name");
        storageAreaNoSpecific.setDescription("storage desc");
        storageAreaNoSpecific.setKind("storage");
    }

    @Test
    public void testCreateOk() {
        ObjectMapper om = new ObjectMapper();
        var storageAreaNode = om.valueToTree(storageArea);
        var outputPortNode = om.valueToTree(outputPort);
        List<JsonNode> nodes = new ArrayList<>();
        nodes.add(storageAreaNode);
        nodes.add(outputPortNode);
        DataProduct dp = new DataProduct();
        dp.setComponents(nodes);
        var provisionRequest = new ProvisionRequest<>(dp, outputPort, false);

        var actualRes = outputPortHandler.create(provisionRequest);

        assertTrue(actualRes.isRight());
        assertEquals("myprefix/healthcare/data-products/vaccinations/0/storage", actualRes.get());
    }

    @Test
    public void testCreateMissingDependentStorageArea() {
        DataProduct dp = new DataProduct();
        var provisionRequest = new ProvisionRequest<>(dp, outputPortNoDepends, false);
        String expectedDesc = "The output port has not a corresponding dependent storage area";

        var actualRes = outputPortHandler.create(provisionRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testCreateWrongComponentType() {
        DataProduct dp = new DataProduct();
        var provisionRequest = new ProvisionRequest<>(dp, storageArea, false);
        String expectedDesc = "The component type is not of expected type OutputPort";

        var actualRes = outputPortHandler.create(provisionRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testCreateUnknownPrefixPath() {
        ObjectMapper om = new ObjectMapper();
        var storageAreaNode = om.valueToTree(storageAreaNoSpecific);
        var outputPortNode = om.valueToTree(outputPort);
        List<JsonNode> nodes = new ArrayList<>();
        nodes.add(storageAreaNode);
        nodes.add(outputPortNode);
        DataProduct dp = new DataProduct();
        dp.setComponents(nodes);
        var provisionRequest = new ProvisionRequest<>(dp, outputPort, false);
        String expectedDesc = "prefixPath not found for the component urn:dmb:cmp:healthcare:vaccinations:0:storage";

        var actualRes = outputPortHandler.create(provisionRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testDestroyOk() {
        DataProduct dp = new DataProduct();
        var provisionRequest = new ProvisionRequest<>(dp, outputPort, false);
        when(rangerService.findRoleByName(anyString())).thenReturn(right(Optional.empty()));

        var actualRes = outputPortHandler.destroy(provisionRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testDestroyUpdateRoleOk() {
        DataProduct dp = new DataProduct();
        var provisionRequest = new ProvisionRequest<>(dp, outputPort, false);
        when(rangerService.findRoleByName(anyString())).thenReturn(right(Optional.of(new RangerRole())));
        when(rangerService.updateRole(any())).thenReturn(right(new RangerRole()));

        var actualRes = outputPortHandler.destroy(provisionRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testDestroyMissingDependentStorageArea() {
        DataProduct dp = new DataProduct();
        var provisionRequest = new ProvisionRequest<>(dp, outputPortNoDepends, false);
        String expectedDesc = "The output port has not a corresponding dependent storage area";

        var actualRes = outputPortHandler.destroy(provisionRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testDestroyWrongComponentType() {
        DataProduct dp = new DataProduct();
        var provisionRequest = new ProvisionRequest<>(dp, storageArea, false);
        String expectedDesc = "The component type is not of expected type OutputPort";

        var actualRes = outputPortHandler.destroy(provisionRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }
}
