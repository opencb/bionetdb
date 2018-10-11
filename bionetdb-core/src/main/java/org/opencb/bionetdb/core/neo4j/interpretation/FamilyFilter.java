//package org.opencb.bionetdb.core.neo4j.interpretation;
//
//import java.util.List;
//
//public class FamilyFilter {
//
//    // Igual habría que hacer listas de nombres y genotupos
//    public String name;
//    public List<String> genotypes;
//
//    public FamilyFilter(String name) {
//        this.name = name;
//    }
//
//    public FamilyFilter(String name, List<String> listOfGenotypes) {
//        this.name = name;
//        this.genotypes = listOfGenotypes;
//    }
//
//    public String getName() {
//        return name;
//    }
//
//    public void setName(String name) {
//        this.name = name;
//    }
//
//    public List<String> getGenotypes() {
//        return genotypes;
//    }
//
//    public void setGenotypes(String name) {
//        this.genotypes = genotypes;
//    }
//
//    @Override
//    public String toString() {
//        final StringBuilder sb = new StringBuilder("FamilyFilter{");
//        sb.append("name='").append(name).append('\'');
//        sb.append(", listOfGenotypes=").append(genotypes);
//        sb.append('}');
//        return sb.toString();
//    }
//}
