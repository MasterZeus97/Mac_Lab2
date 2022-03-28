package ch.heig.mac;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class Requests {
    private static final  Logger LOGGER = Logger.getLogger(Requests.class.getName());
    private final Driver driver;

    public Requests(Driver driver) {
        this.driver = driver;
    }

    public List<String> getDbLabels() {
        var dbVisualizationQuery = "CALL db.labels";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list(t -> t.get("label").asString());
        }
    }

    public List<Record> possibleSpreaders() {
        var dbVisualizationQuery = "match(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)<-[vH:VISITS]-(pH:Person{healthstatus: 'Healthy'})\n" +
                "where vH.starttime > pSick.confirmedtime and vSick.starttime > pSick.confirmedtime\n" +
                "return distinct pSick.name as sickName";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> possibleSpreadCounts() {
        var dbVisualizationQuery = "match(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)<-[vH:VISITS]-(pH:Person{healthstatus: 'Healthy'})\n" +
                "where vH.starttime > pSick.confirmedtime and vSick.starttime > pSick.confirmedtime\n" +
                "return distinct pSick.name as sickName, size(collect(distinct pH.name)) as nbHealthy";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> carelessPeople() {
        var dbVisualizationQuery =
                "MATCH (n:Person{healthstatus:'Sick'})-[v:VISITS]-(p:Place)\n" +
                "WHERE v.starttime > n.confirmedtime\n" +
                "WITH n, size(collect(p.name)) AS nbPlaces\n" +
                "WHERE nbPlaces > 10\n" +
                "RETURN n.name AS sickName, nbPlaces\n" +
                "ORDER BY nbPlaces";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> sociallyCareful() {
        var dbVisualizationQuery = "match(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)\n" +
                "where vSick.starttime > pSick.confirmedtime\n" +
                "with collect(distinct pSick) as sickeWithBar\n" +
                "match (p:Person{healthstatus: 'Sick'})\n" +
                "with collect(distinct p) as sickPeople, sickeWithBar\n" +
                "with apoc.coll.subtract(sickPeople, sickeWithBar) as sickWithoutBar\n" +
                "unwind sickWithoutBar as tmp\n" +
                "return tmp.name as sickName";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> peopleToInform() {
        var dbVisualizationQuery = "match(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)<-[vH:VISITS]-(pH:Person{healthstatus: 'Healthy'})\n" +
                "where vSick.starttime > pSick.confirmedtime and vH.endtime > vSick.starttime and datetime() + duration.inSeconds(apoc.coll.max([vSick.starttime, vH.starttime]), apoc.coll.min([vSick.endtime, vH.endtime])) >= datetime() + duration(\"PT2H\")\n" +
                "return distinct pSick.name as sickName, collect(pH.name) as peopleToInform";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> setHighRisk() {
        var dbVisualizationQuery = "match(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)<-[vH:VISITS]-(pH:Person{healthstatus: 'Healthy'})\n" +
                "where vSick.starttime > pSick.confirmedtime and vH.endtime > vSick.starttime and datetime() + duration.inSeconds(apoc.coll.max([vSick.starttime, vH.starttime]), apoc.coll.min([vSick.endtime, vH.endtime])) >= datetime() + duration(\"PT2H\")\n" +
                "with pSick.name as malade, collect(pH) as persons\n" +
                "unwind persons as listOfRisk\n" +
                "foreach(n in persons | set n.risk = \"high\")\n" +
                "return distinct listOfRisk.name as highRiskName";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> healthyCompanionsOf(String name) {
        var dbVisualizationQuery =
                "match(p:Person{name:$name})-[v:VISITS]->(k:Place)<-[u:VISITS]-(o:Person{healthstatus: 'Healthy'})\n" +
                "with collect(distinct o) as oc\n" +
                "unwind oc as ou\n" +
                "match(ou)-[v:VISITS]->(k:Place)<-[u:VISITS]-(o:Person{healthstatus: 'Healthy'})\n" +
                "with collect(distinct o) as occ\n" +
                "unwind occ as ouu\n" +
                "match(ouu)-[v:VISITS]->(k:Place)<-[u:VISITS]-(o:Person{healthstatus: 'Healthy'})\n" +
                "with collect(distinct o) as occc\n" +
                "unwind occc as ouuu\n" +
                "return ouuu.name AS healthyName";
        Map<String,Object> params = new HashMap<>();
        params.put( "name", name );

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery, params);
            return result.list();
        }
    }

    public Record topSickSite() {
        var dbVisualizationQuery = "match(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)\n" +
                "with place, size(collect(distinct pSick)) as nbOfSickVisits\n" +
                "return place.type as placeType, nbOfSickVisits\n" +
                "order by nbOfSickVisits desc\n" +
                "limit 1";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.single();
        }
    }

    public List<Record> sickFrom(List<String> names) {
        var dbVisualizationQuery =
                "MATCH (per:Person{healthstatus:'Sick'})\n" +
                "WHERE per.name IN $lst " +
                "RETURN per.name AS sickName";
        Map<String,Object> params = new HashMap<>();
        params.put( "lst", names );

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery, params);
            return result.list();
        }
    }
}
