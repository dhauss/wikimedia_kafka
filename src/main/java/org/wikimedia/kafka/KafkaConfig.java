package org.wikimedia.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class KafkaConfig {
    private String bootstrapServer;
    private String saslMechanism;
    private String securityProtocol;
    private String saslJassConfig;

    public KafkaConfig(String filepath){
        try{
            File config_file = new File(filepath);
            FileReader config_fr = new FileReader(config_file);
            BufferedReader config_br = new BufferedReader((config_fr));
            StringBuffer config_sb = new StringBuffer();
            String line;

            while((line = config_br.readLine()) != null){
                config_sb.append(line);
                config_sb.append("\n");
            }
            config_fr.close();

            String[] configStrings = config_sb.toString().split("\n");

            this.setBootstrapServer(configStrings[0].split("=")[1]);
            this.setSaslMechanism(configStrings[1].split("=")[1]);
            this.setSecurityProtocol(configStrings[2].split("=")[1]);

            String[] saslConfig = configStrings[3].split("=");
            this.setSaslJassConfig(saslConfig[1] + "=" + saslConfig[2] + "=" + saslConfig[3]);

        } catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        KafkaConfig kc = new KafkaConfig("src/main/resources/config.txt");
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public void setSaslJassConfig(String saslJassConfig) {
        this.saslJassConfig = saslJassConfig;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getSaslJassConfig() {
        return saslJassConfig;
    }
}
