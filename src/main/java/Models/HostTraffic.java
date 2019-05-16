package Models;

public class HostTraffic {
    String sourceHost;
    String targetHost;
    Integer trafficAmount;

    public String getSourceHost() {
        return sourceHost;
    }

    public void setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
    }

    public String getTargetHost() {
        return targetHost;
    }

    public void setTargetHost(String targetHost) {
        this.targetHost = targetHost;
    }

    public Integer getTrafficAmount() {
        return trafficAmount;
    }

    public void setTrafficAmount(Integer trafficAmount) {
        this.trafficAmount = trafficAmount;
    }

}
