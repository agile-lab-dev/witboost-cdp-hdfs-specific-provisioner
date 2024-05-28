package it.agilelab.witboost.cdp.priv.hdfs.provisioning.model;

import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class StorageSpecificTest {

    @Test
    void getPath() {
        var specific = new StorageSpecific();
        specific.setRootFolder("prefixPath/healthcare/data-products/vaccinations/0");
        specific.setFolder("storage");

        assertEquals(right("prefixPath/healthcare/data-products/vaccinations/0/storage"), specific.getPath());
    }
}
