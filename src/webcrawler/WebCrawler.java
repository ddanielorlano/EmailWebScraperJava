package webcrawler;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class WebCrawler {

    private final Set<String> linksToVisit;
    private final Set<String> linksVisited;
    private final Set<String> emailSet;
    private final String START_URL = "http://www.touro.edu/";
    private final ExecutorService pool;
    private final int EMAIL_MAX_COUNT = 10000;
    private DbUpdater database;

    WebCrawler() throws MalformedURLException, IOException {
        pool = Executors.newFixedThreadPool(110);

        linksToVisit = Collections.synchronizedSet(new HashSet<String>());
        linksVisited = Collections.synchronizedSet(new HashSet<String>());
        emailSet = Collections.synchronizedSet(new HashSet<String>());

        linksToVisit.add(START_URL);
        String link = "";

        while ((!linksToVisit.isEmpty() || !pool.isTerminated())
                && !(emailSet.size() >= EMAIL_MAX_COUNT)) {

            synchronized (linksToVisit) {
                Iterator i = linksToVisit.iterator();
                if (i.hasNext()) {
                    link = (String) i.next();
                    i.remove();                   
                }                
            }
            if (!linksVisited.contains(link)) {//only scrape the link if it hasnt been visited
                linksVisited.add(link);
                System.out.println(link + " : " + emailSet.size());
                pool.execute(new Scraper(link, linksToVisit, emailSet));
            }
        }
        pool.shutdownNow();       
        addEmailsToDb();
    }
    

    private void addEmailsToDb() throws IOException {
        System.out.println("Adding emails to the databse");
        database = new DbUpdater(emailSet);
        database.InsertEmails();
    }

    class Scraper implements Runnable {

        private String link;
        private Document doc;
        private Set<String> linksToVisit;
        private Set<String> emailSet;
        private final String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64)"
                + " AppleWebKit/537.36 (KHTML, like Gecko) "
                + "Chrome/42.0.2311.152 Safari/537.36";

        public Scraper(String link, Set<String> 
                linksToVisit, Set<String> emailSet) throws IOException {
            this.link = link;
            this.linksToVisit = linksToVisit;
            this.emailSet = emailSet;

        }

        @Override
        public void run() {
            if (connectToLink()) {
                scrapeLinks();
                scrapeEmails();
            }
        }

        private boolean connectToLink() {
            try {
                doc = Jsoup.connect(link)
                        .userAgent(USER_AGENT)
                        .referrer("http://www.google.com")
                        .timeout(12000)
                        .followRedirects(true).get();
                return true;
            } catch (IOException | IllegalArgumentException ex) {
                 // System.out.println(ex.toString());
                return false;
            }
        }

        public void scrapeLinks() {

            Set<String> mySet = new HashSet<>();

            Elements links = doc.select("a[href]");
            for (Element e : links) {
                mySet.add(e.attr("abs:href"));
            }

            mySet.removeAll(linksVisited);

            Iterator i = mySet.iterator();
            while (i.hasNext()) {
                String str = (String) i.next();
                if (str.contains(".pdf") || (str.contains("mailto") || (str.contains(".jpg")))) {
                    i.remove();
                }
            }
            linksToVisit.addAll(mySet);

        }

        public void scrapeEmails() {
            Set<String> mySet = new HashSet<>();

            Elements emails = doc.getElementsMatchingOwnText("^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$");

            for (Element e : emails) {
                mySet.add(e.text());
            }

            mySet.removeAll(emailSet);
            emailSet.addAll(mySet);
        }
    
    }
    public static void main(String[] args) throws IOException {
        new WebCrawler();
    }

}
