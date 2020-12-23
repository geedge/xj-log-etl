import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class UrlTest {
    public static void main(String[] args) throws IOException, URISyntaxException {
//        URL url = new URL("api.miwifi.com/config");
       // System.out.println(url);
/*        URI uri = new URI("11.tlu.dl.delivery.mp.microsoft.com/filestreamingservice/files/a3c2ea90-aa2e-46e7-bff7-ffda765fcad1?P1=1607615917&P2=402&P3=2&P4=PwjRqKU7aZciz4tcczBUIcaXYCfjOo1tTZ%2bfrxMBcDMqTIp5QImIOnygj0R8TnbbebaMC%2fkDgTTrgFMPuMW6Cw%3d%3d");
        System.out.println(uri.getPath());*/
        String s = "http://"+"11.tlu.dl.delivery.mp.microsoft.com/filestreamingservice/files/a3c2ea90-aa2e-46e7-bff7-ffda765fcad1?P1=1607615917&P2=402&P3=2&P4=PwjRqKU7aZciz4tcczBUIcaXYCfjOo1tTZ%2bfrxMBcDMqTIp5QImIOnygj0R8TnbbebaMC%2fkDgTTrgFMPuMW6Cw%3d%3d";
        System.out.println(s);
        URL url = new URL(s);
        System.out.println(url.getPath());
    }
}
