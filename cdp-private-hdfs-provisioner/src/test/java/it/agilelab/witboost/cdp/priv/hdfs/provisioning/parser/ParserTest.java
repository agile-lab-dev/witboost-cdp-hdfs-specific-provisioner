package it.agilelab.witboost.cdp.priv.hdfs.provisioning.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Descriptor;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Specific;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.StorageSpecific;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.util.ResourceUtils;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class ParserTest {

    @Test
    public void testParseStorageDescriptorOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptor_storage_ok.yml");

        var actualRes = Parser.parseDescriptor(ymlDescriptor);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testParseOutputPortDescriptorOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptor_outputport_ok.yml");

        var actualRes = Parser.parseDescriptor(ymlDescriptor);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testParseStorageDescriptorFail() {
        String invalidDescriptor = "an_invalid_descriptor";
        String expectedDesc = "Failed to deserialize the Yaml Descriptor. Details: ";

        var actualRes = Parser.parseDescriptor(invalidDescriptor);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertTrue(p.description().startsWith(expectedDesc));
            assertTrue(p.cause().isPresent());
        });
    }

    @Test
    public void testParseStorageComponentOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptor_storage_ok.yml");
        var eitherDescriptor = Parser.parseDescriptor(ymlDescriptor);
        assertTrue(eitherDescriptor.isRight());
        Descriptor descriptor = eitherDescriptor.get();
        String componentIdToProvision = "urn:dmb:cmp:healthcare:vaccinations:0:storage";
        var optionalComponent = descriptor.getDataProduct().getComponentToProvision(componentIdToProvision);
        assertTrue(optionalComponent.isDefined());
        JsonNode component = optionalComponent.get();

        var actualRes = Parser.parseComponent(component, StorageSpecific.class);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testParseOutputPortComponentOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptor_outputport_ok.yml");
        var eitherDescriptor = Parser.parseDescriptor(ymlDescriptor);
        assertTrue(eitherDescriptor.isRight());
        Descriptor descriptor = eitherDescriptor.get();
        String componentIdToProvision = "urn:dmb:cmp:healthcare:vaccinations:0:hdfs-output-port";
        var optionalComponent = descriptor.getDataProduct().getComponentToProvision(componentIdToProvision);
        assertTrue(optionalComponent.isDefined());
        JsonNode component = optionalComponent.get();

        var actualRes = Parser.parseComponent(component, Specific.class);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testParseStorageComponentFail() {
        JsonNode node = null;
        String expectedDesc = "Failed to deserialize the component. Details: ";

        var actualRes = Parser.parseComponent(node, StorageSpecific.class);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertTrue(p.description().startsWith(expectedDesc));
            assertTrue(p.cause().isPresent());
        });
    }
}
