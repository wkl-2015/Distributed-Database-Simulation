import java.util.List;

/**
 * Created by hongdaj on 11/26/14.
 */
public class DataManager {

    private List<Site> sites;

    public DataManager(List<Site> sites){
        this.sites = sites;
    }

    public int read(int varId) {
        if(varId % 2 == 0) {
            for (int i = 0; i < 10; i++) {
                if (sites.get(i).isUp()) {
                    int value = sites.get(i).getVar(varId).getValue();
                    return value;
                }
            }
            throw new RuntimeException("All sites are down!");
        }
        else{
            int siteId = varId % 10 + 1;
            if(sites.get(siteId - 1).isUp()){
                int value = sites.get(siteId - 1).getVar(varId).getValue();
                return value;
            }
            throw new RuntimeException("Site " + siteId + " is down!");
        }

    }

    public void write(int varId, int value) {
        if(varId % 2 == 0){
            for(int i = 0; i < 10; i++){
                if(sites.get(i).isUp()){
                    sites.get(i).updatePendingValue(varId, value);
                }
            }
        }
        else{
            int siteId = varId % 10 + 1;
            if(sites.get(siteId - 1).isUp()){
                sites.get(siteId - 1).updatePendingValue(varId, value);
            }
        }
        System.out.println("Pending write " + value + " to x" + varId);
    }

    public void commit(int varId){
        if(varId % 2 == 0){
            for(int i = 0; i < 10; i++){
                if(sites.get(i).isUp()){
                    sites.get(i).getVar(varId).writePendingValueToValue();
                }
            }
        }
        else{
            int siteId = varId % 10 + 1;
            if(sites.get(siteId - 1).isUp()){
                sites.get(siteId - 1).getVar(varId).writePendingValueToValue();
            }
        }
        System.out.println("Write to x" + varId + " successfully");

    }

    public void dumpAll() {
        for(Site site: sites){
            System.out.print("Site " + site.getId() + " : ");
            for(int i = 1; i < 21; i++){
                if(site.containsVar(i)) {
                    System.out.print("x" + i + "=" + site.getVar(i).getValue() + " ");
                }
            }
            System.out.println(";");
        }
    }

    public void dumpSite(int siteId){
        Site site = sites.get(siteId - 1);
        System.out.print("Site " + site.getId() + " : ");
        for(int i = 1; i < 21; i++){
            if(site.containsVar(i)) {
                System.out.print("x" + i + "=" + site.getVar(i) + " ");
            }
        }
        System.out.println(";");
    }

    public void dumpVariable(int varId){
        if(varId % 2 == 0) {
            for (Site site : sites) {
                System.out.print("Site " + site.getId() + " : ");
                System.out.print("x" + varId + "=" + site.getVar(varId) + " ");
                System.out.println(";");
            }
        }
        else{
            int siteId = varId % 10 + 1;
            if(sites.get(siteId - 1).containsVar(varId)){
                int value = sites.get(siteId - 1).getVar(varId).getValue();
                System.out.println("Site " + siteId + ": " + "x" + varId +
                    " = " + value + ";");
            }

        }
    }

    public void fail(int siteId) {
        sites.get(siteId - 1).setDown();
    }

    public void recover(int siteId) {
        sites.get(siteId - 1).setUp();
    }

    public boolean isAllSiteDown() {
        for (Site site : sites) {
            if (site.isUp()) {
                return false;
            }
        }
        return true;
    }
}
