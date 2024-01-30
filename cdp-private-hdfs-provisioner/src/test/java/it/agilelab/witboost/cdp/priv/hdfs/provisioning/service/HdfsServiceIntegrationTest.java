package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.HdfsConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.client.RestTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@SpringBootTest(classes = {RestTemplateAutoConfiguration.class})
@Testcontainers
public class HdfsServiceIntegrationTest {

    @Container
    static GenericContainer<?> miniclusterContainer =
            new GenericContainer<>(
                            new ImageFromDockerfile("minidfscluster", false)
                                    .withFileFromClasspath("Dockerfile", "minicluster/Dockerfile"))
                    .withExposedPorts(9000, 9001)
                    .waitingFor(Wait.forLogMessage(".*Started MiniDFSCluster -- namenode on port.*", 1))
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("minicluster/hdfs-site.xml"),
                            "/work/hadoop-3.3.6/etc/hadoop/hdfs-site.xml");

    @Autowired private RestTemplateBuilder restTemplateBuilder;

    @Test
    void testMiniclusterIsRunning() {
        assertThat(miniclusterContainer.isRunning()).isTrue();
    }

    @Test
    void testIntegrationHdfsServiceCreateAndDeleteIsWorking() {
        String url =
                String.format(
                        "http://%s:%s?user.name=root",
                        miniclusterContainer.getHost(), miniclusterContainer.getMappedPort(9001));
        HdfsConfig integrationHdfsConfig = new HdfsConfig(url, 10);
        HdfsService hdfsService =
                new HdfsServiceImpl(restTemplateBuilder.build(), integrationHdfsConfig);

        var resCreation = hdfsService.createFolder("myfolder");

        assertThat(resCreation.isRight()).isTrue();
        assertEquals("myfolder", resCreation.get());

        var resDeletion = hdfsService.deleteFolder("myfolder");

        assertThat(resDeletion.isRight()).isTrue();
        assertEquals("myfolder", resDeletion.get());
    }

    @Test
    void testIntegrationHdfsServiceDeleteAlreadyDeletedIsWorking() {
        String url =
                String.format(
                        "http://%s:%s?user.name=root",
                        miniclusterContainer.getHost(), miniclusterContainer.getMappedPort(9001));
        HdfsConfig integrationHdfsConfig = new HdfsConfig(url, 10);
        HdfsService hdfsService =
                new HdfsServiceImpl(restTemplateBuilder.build(), integrationHdfsConfig);

        var resDeletion = hdfsService.deleteFolder("notexisting");

        assertThat(resDeletion.isRight()).isTrue();
        assertEquals("notexisting", resDeletion.get());
    }
}
