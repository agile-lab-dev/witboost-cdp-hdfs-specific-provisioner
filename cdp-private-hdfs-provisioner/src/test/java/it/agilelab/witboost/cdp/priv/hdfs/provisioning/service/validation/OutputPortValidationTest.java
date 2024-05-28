package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class OutputPortValidationTest {

    OutputPortValidation outputPortValidation = new OutputPortValidation();

    @Test
    public void testValidateOk() {
        StorageSpecific storageSpecific = new StorageSpecific();
        storageSpecific.setRootFolder("myprefix/healthcare/data-products/vaccinations/0");
        storageSpecific.setFolder("storage");
        StorageArea<StorageSpecific> storageArea = new StorageArea<>();
        storageArea.setId("my_id_storage");
        storageArea.setName("storage name");
        storageArea.setDescription("storage desc");
        storageArea.setKind("storage");
        storageArea.setSpecific(storageSpecific);

        Specific specific = new Specific();
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setId("my_id_outputport");
        outputPort.setName("outputport name");
        outputPort.setDescription("outputport desc");
        outputPort.setKind("outputport");
        outputPort.setDependsOn(Collections.singletonList("my_id_storage"));
        outputPort.setSpecific(specific);

        DataProduct dataProduct = new DataProduct();
        dataProduct.setId("my_dp");
        ObjectMapper om = new ObjectMapper();
        var storageAreaNode = om.valueToTree(storageArea);
        var outputPortNode = om.valueToTree(outputPort);
        List<JsonNode> nodes = new ArrayList<>();
        nodes.add(storageAreaNode);
        nodes.add(outputPortNode);
        dataProduct.setComponents(nodes);

        var actualRes = outputPortValidation.validate(dataProduct, outputPort);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateWrongType() {
        StorageArea<StorageSpecific> storageArea = new StorageArea<>();
        storageArea.setId("my_id_storage");

        DataProduct dataProduct = new DataProduct();
        dataProduct.setId("my_dp");
        ObjectMapper om = new ObjectMapper();
        var storageAreaNode = om.valueToTree(storageArea);
        List<JsonNode> nodes = new ArrayList<>();
        nodes.add(storageAreaNode);
        dataProduct.setComponents(nodes);

        String expectedDesc = "The component my_id_storage is not of type OutputPort";

        var actualRes = outputPortValidation.validate(dataProduct, storageArea);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongDepends() {
        Specific specific = new Specific();
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setId("my_id_outputport");
        outputPort.setName("outputport name");
        outputPort.setDescription("outputport desc");
        outputPort.setKind("outputport");
        outputPort.setDependsOn(Collections.emptyList());
        outputPort.setSpecific(specific);

        DataProduct dataProduct = new DataProduct();
        dataProduct.setId("my_dp");
        ObjectMapper om = new ObjectMapper();
        var outputPortNode = om.valueToTree(outputPort);
        List<JsonNode> nodes = new ArrayList<>();
        nodes.add(outputPortNode);
        dataProduct.setComponents(nodes);

        String expectedDesc = "Expected exactly a dependency for the component my_id_outputport, found: 0";

        var actualRes = outputPortValidation.validate(dataProduct, outputPort);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateNotFoundDepends() {
        Specific specific = new Specific();
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setId("my_id_outputport");
        outputPort.setName("outputport name");
        outputPort.setDescription("outputport desc");
        outputPort.setKind("outputport");
        outputPort.setDependsOn(Collections.singletonList("my_id_storage"));
        outputPort.setSpecific(specific);

        DataProduct dataProduct = new DataProduct();
        dataProduct.setId("my_dp");
        ObjectMapper om = new ObjectMapper();
        var outputPortNode = om.valueToTree(outputPort);
        List<JsonNode> nodes = new ArrayList<>();
        nodes.add(outputPortNode);
        dataProduct.setComponents(nodes);

        String expectedDesc = "OutputPort's dependency my_id_storage not found in the Descriptor";

        var actualRes = outputPortValidation.validate(dataProduct, outputPort);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongKindDepends() {
        Specific specific = new Specific();
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setId("my_id_outputport");
        outputPort.setName("outputport name");
        outputPort.setDescription("outputport desc");
        outputPort.setKind("outputport");
        outputPort.setDependsOn(Collections.singletonList("my_id_outputport2"));
        outputPort.setSpecific(specific);

        OutputPort<Specific> outputPort2 = new OutputPort<>();
        outputPort2.setId("my_id_outputport2");
        outputPort2.setName("outputport name");
        outputPort2.setDescription("outputport desc");
        outputPort2.setKind("outputport");

        DataProduct dataProduct = new DataProduct();
        dataProduct.setId("my_dp");
        ObjectMapper om = new ObjectMapper();
        var outputPortNode = om.valueToTree(outputPort);
        var outputPortNode2 = om.valueToTree(outputPort2);
        List<JsonNode> nodes = new ArrayList<>();
        nodes.add(outputPortNode);
        nodes.add(outputPortNode2);
        dataProduct.setComponents(nodes);

        String expectedDesc =
                "Kind of dependent component my_id_outputport2 is not right. Expected: storage, found: outputport";

        var actualRes = outputPortValidation.validate(dataProduct, outputPort);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }
}
