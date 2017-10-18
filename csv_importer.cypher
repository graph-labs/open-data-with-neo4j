USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///entreprises.csv" AS line
FIELDTERMINATOR ','
MERGE (c:Company {company: line.denomination_sociale})
MERGE (s:Sector {sector_code: line.secteur_activite_code, sector: line.secteur})
MERGE (co:Country {country: line.pays, country_code:line.pays_code})
MERGE (ad:Address {address_1: line.adresse_1, address_2: line.adresse_2, address_3: line.adresse_3, address_4: line.adresse_4})
MERGE (pc: PostalCode {postal_code: line.code_postal})
MERGE (city: City {city: line.ville})


MERGE (c)-[:IS_PART_OF_SECTOR]->(s)
MERGE (c)-[:IS_LOCATED_IN]->(ad)
MERGE (ad)-[:IS_ADDRESS_IN]->(pc)
MERGE (pc)-[:IDENTIFIES_ZONE_OF]->(city)
MERGE (city)-[:BELONGS_TO]->(co)



