package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.OutputPort;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Specific;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.StorageArea;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.StorageSpecific;
import org.junit.jupiter.api.Test;

public class StorageAreaValidationTest {

    @Test
    public void testValidateOk() {
        StorageSpecific storageSpecific = new StorageSpecific();
        storageSpecific.setPrefixPath("myprefix");
        StorageArea<StorageSpecific> storageArea = new StorageArea<>();
        storageArea.setId("my_id_storage");
        storageArea.setName("storage name");
        storageArea.setDescription("storage desc");
        storageArea.setKind("storage");
        storageArea.setSpecific(storageSpecific);

        var actualRes = StorageAreaValidation.validate(storageArea);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateWrongType() {
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setId("my_id_storage");
        String expectedDesc = "The component my_id_storage is not of type StorageArea";

        var actualRes = StorageAreaValidation.validate(outputPort);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertEquals(expectedDesc, p.description());
                            assertTrue(p.cause().isEmpty());
                        });
    }

    @Test
    public void testValidateWrongSpecific() {
        Specific specific = new Specific();
        StorageArea<Specific> storageArea = new StorageArea<>();
        storageArea.setId("my_id_storage");
        storageArea.setName("storage name");
        storageArea.setDescription("storage desc");
        storageArea.setKind("storage");
        storageArea.setSpecific(specific);
        String expectedDesc =
                "The specific section of the component my_id_storage is not of type StorageSpecific";

        var actualRes = StorageAreaValidation.validate(storageArea);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertEquals(expectedDesc, p.description());
                            assertTrue(p.cause().isEmpty());
                        });
    }

    @Test
    public void testValidateInvalidPrefixPath() {
        StorageSpecific storageSpecific = new StorageSpecific();
        StorageArea<StorageSpecific> storageArea = new StorageArea<>();
        storageArea.setId("my_id_storage");
        storageArea.setName("storage name");
        storageArea.setDescription("storage desc");
        storageArea.setKind("storage");
        storageArea.setSpecific(storageSpecific);
        String expectedDesc = "Invalid 'prefixPath' for the component my_id_storage";

        var actualRes = StorageAreaValidation.validate(storageArea);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertEquals(expectedDesc, p.description());
                            assertTrue(p.cause().isEmpty());
                        });
    }
}
