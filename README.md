## Datasources

 - [ANSM Database](http://base-donnees-publique.medicaments.gouv.fr/telechargement.php)
 - [ETALab lab<->doctor gift exports](https://www.transparence.sante.gouv.fr/exports-etalab/exports-etalab.zip)

## Import example

### First: Labs

```
 $> java -jar import/target/import.jar companies --defined-in ~/path/to/entreprise_xxx.csv \
                                                 --to-graph bolt://localhost:7687 \
                                                 --username neo4j \
                                                 --password
 Neo4j password:
```

### Second: Drugs

First, copy the extension JAR:
```
 $> mvn -am -pl similarity-extension clean package \
    && neo4j stop \
    && cp similarity-extension/target/similarity-extension.jar $NEO4J_HOME/plugins \
    && neo4j start
```

Then, convert the file to UTF-8:

```
 $> iconv -f ISO-8859-1 -t UTF-8//TRANSLIT /path/to/CIS_bdpm.txt -o /path/to/CIS_bdpm.utf8.txt 
```

Then, execute the import:
```
 $> java -jar import/target/import.jar drugs --defined-in ~/path/to/CIS_bdpm.utf8.txt \
                                             --to-graph bolt://localhost:7687 \
                                             --username neo4j \
                                             --password
 Neo4j password:
```

### Third: packages

First, convert the file to UTF-8:

```
 $> iconv -f ISO-8859-1 -t UTF-8//TRANSLIT /path/to/CIS_CIP_bdpm.txt -o /path/to/CIS_CIP_bdpm.utf8.txt
```

```
 $> java -jar import/target/import.jar packages --defined-in ~/path/to/CIS_CIP_bdpm.utf8.txt \
                                                --to-graph bolt://localhost:7687 \
                                                --username neo4j \
                                                --password
 Neo4j password:
```

### Fourth: benefits

```
 $> java -jar import/target/import.jar benefits --defined-in ~/path/to/declaration_avantage_XXX.csv \
                                                --to-graph bolt://localhost:7687 \
                                                --username neo4j \
                                                --password
 Neo4j password:
```

## Starting

Run `mvn -am -pl api clean package` first if necessary.

```
 $> export NEO4J_PASSWORD=s3cr3t; java -jar api/target/api-1.0.0-SNAPSHOT.jar
```

Other available envvars:

 - `NEO4J_ADDRESS` (Bolt URI, defaults to local one)
 - `NEO4J_USER` (defaults to `neo4j`)

Available APIs:

 - `/packages/[package-name]/labs`
 - `/health-professionals/[first-name]/[last-name]/benefits`
 - `/benefits/[year]/health-professionals`
