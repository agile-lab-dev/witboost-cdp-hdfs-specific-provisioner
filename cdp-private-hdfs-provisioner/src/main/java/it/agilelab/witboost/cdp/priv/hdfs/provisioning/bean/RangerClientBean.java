package it.agilelab.witboost.cdp.priv.hdfs.provisioning.bean;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.RangerConfig;
import org.apache.ranger.RangerClient;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class RangerClientBean {

    @Bean
    public RangerClient rangerClient(RangerConfig rangerConfig) {
        return new RangerClient(
                rangerConfig.baseUrl(), "simple", rangerConfig.username(), rangerConfig.password(), "");
    }
}
