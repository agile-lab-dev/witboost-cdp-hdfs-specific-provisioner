package it.agilelab.witboost.cdp.priv.hdfs.provisioning.rest;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.KerberosConfig;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.RangerConfig;
import org.apache.ranger.RangerClient;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class RangerClientBean {

    @Bean
    public RangerClient rangerClient(KerberosConfig kerberosConfig, RangerConfig rangerConfig) {
        return new RangerClient(
                rangerConfig.baseUrl(),
                "kerberos",
                kerberosConfig.principal(),
                kerberosConfig.keytabLocation(),
                "");
    }
}
