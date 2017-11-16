/**
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.graphlabs.neo4j.companies

import fr.graphlabs.neo4j.CommitCounter
import fr.graphlabs.neo4j.Readers.newReader
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.groups.Tuple
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.harness.junit.Neo4jRule
import org.slf4j.bridge.SLF4JBridgeHandler

class CompanyImporterTest {

    @get:Rule
    val graphDb = Neo4jRule()

    private lateinit var subject: CompanyImporter

    private lateinit var commitCounter: CommitCounter

    @Before
    fun prepare() {
        subject = CompanyImporter(graphDb.boltURI().toString())
        commitCounter = CommitCounter()
        graphDb.graphDatabaseService.registerTransactionEventHandler(commitCounter)
    }

    companion object {
        @JvmStatic
        @BeforeClass
        fun prepareAll() {
            SLF4JBridgeHandler.removeHandlersForRootLogger()
        }
    }

    @Test
    fun `imports countries of companies`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute(
                "MATCH (country:Country) " +
                        "RETURN country {.code, .name} " +
                        "ORDER BY country.code ASC").use {

            assertThat(it).containsExactly(
                    row("country", mapOf(Pair("code", "[FR]"), Pair("name", "FRANCE"))),
                    row("country", mapOf(Pair("code", "[US]"), Pair("name", "ÉTATS-UNIS")))
            )
        }
        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports cities`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute(
                "MATCH (city:City) " +
                        "RETURN city {.name} " +
                        "ORDER BY city.name ASC").use {

            assertThat(it).containsExactly(
                    row("city", mapOf(Pair("name", "BOULOGNE BILLANCOURT"))),
                    row("city", mapOf(Pair("name", "CHARENTON LE PONT CEDEX"))),
                    row("city", mapOf(Pair("name", "HONFLEUR"))),
                    row("city", mapOf(Pair("name", "LYON"))),
                    row("city", mapOf(Pair("name", "MAHWAH"))),
                    row("city", mapOf(Pair("name", "RILLIEUX-LA-PAPE"))),
                    row("city", mapOf(Pair("name", "SAINT-JUST SAINT-RAMBERT CEDEX"))),
                    row("city", mapOf(Pair("name", "STAINS")))
            )
        }
        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports city|country links`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute(
                "MATCH (city:City)-[:LOCATED_IN_COUNTRY]->(country:Country) " +
                        "RETURN country {.code}, city {.name} " +
                        "ORDER BY city.name ASC").use {

            assertThat(it).containsExactly(
                    mapOf(Pair("country", mapOf(Pair("code", "[FR]"))), Pair("city", mapOf(Pair("name", "BOULOGNE BILLANCOURT")))),
                    mapOf(Pair("country", mapOf(Pair("code", "[FR]"))), Pair("city", mapOf(Pair("name", "CHARENTON LE PONT CEDEX")))),
                    mapOf(Pair("country", mapOf(Pair("code", "[FR]"))), Pair("city", mapOf(Pair("name", "HONFLEUR")))),
                    mapOf(Pair("country", mapOf(Pair("code", "[FR]"))), Pair("city", mapOf(Pair("name", "LYON")))),
                    mapOf(Pair("country", mapOf(Pair("code", "[US]"))), Pair("city", mapOf(Pair("name", "MAHWAH")))),
                    mapOf(Pair("country", mapOf(Pair("code", "[FR]"))), Pair("city", mapOf(Pair("name", "RILLIEUX-LA-PAPE")))),
                    mapOf(Pair("country", mapOf(Pair("code", "[FR]"))), Pair("city", mapOf(Pair("name", "SAINT-JUST SAINT-RAMBERT CEDEX")))),
                    mapOf(Pair("country", mapOf(Pair("code", "[FR]"))), Pair("city", mapOf(Pair("name", "STAINS"))))
            )
        }
        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports addresses`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute(
                "MATCH (address:Address) " +
                        "RETURN address {.address} ").use {

            assertThat(it).containsOnlyOnce(
                    row("address", mapOf(Pair("address", "16 RUE DE MONTBRILLANT\nBUROPARC RIVE GAUCHE"))),
                    row("address", mapOf(Pair("address", "ZI SUD D'ANDREZIEUX\nRUE B. THIMONNIER"))),
                    row("address", mapOf(Pair("address", "47 BOULEVARD CHARLES V"))),
                    row("address", mapOf(Pair("address", "162 AVENUE JEAN JAURÈS"))),
                    row("address", mapOf(Pair("address", "10-16 RUE DU COLONEL ROL TANGUY\nZAC DU BOIS MOUSSAY"))),
                    row("address", mapOf(Pair("address", "8 RUE DE L'EST"))),
                    row("address", mapOf(Pair("address", "800 CORPORATE DRIVE"))),
                    row("address", mapOf(Pair("address", "2 RUE DUE NOUVEAU BERCY"))),
                    row("address", mapOf(Pair("address", "2871 AVENUE DE L'EUROPE")))
            )
        }
        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports address|city links`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute(
                "MATCH (address:Address)-[location:LOCATED_IN_CITY]->(city:City) " +
                        "RETURN location {.zipcode}, city {.name}, address {.address} " +
                        "ORDER BY location.zipcode ASC").use {

            assertThat(it).containsOnlyOnce(
                    mapOf(
                            Pair("location", mapOf(Pair("zipcode", "07430"))),
                            Pair("city", mapOf(Pair("name", "MAHWAH"))),
                            Pair("address", mapOf(Pair("address", "800 CORPORATE DRIVE")))
                    ),
                    mapOf(
                            Pair("location", mapOf(Pair("zipcode", "14600"))),
                            Pair("city", mapOf(Pair("name", "HONFLEUR"))),
                            Pair("address", mapOf(Pair("address", "47 BOULEVARD CHARLES V")))
                    ),
                    mapOf(
                            Pair("location", mapOf(Pair("zipcode", "42173"))),
                            Pair("city", mapOf(Pair("name", "SAINT-JUST SAINT-RAMBERT CEDEX"))),
                            Pair("address", mapOf(Pair("address", "ZI SUD D'ANDREZIEUX\nRUE B. THIMONNIER")))
                    ),
                    mapOf(
                            Pair("location", mapOf(Pair("zipcode", "69003"))),
                            Pair("city", mapOf(Pair("name", "LYON"))),
                            Pair("address", mapOf(Pair("address", "16 RUE DE MONTBRILLANT\nBUROPARC RIVE GAUCHE")))
                    ),
                    mapOf(
                            Pair("location", mapOf(Pair("zipcode", "69007"))),
                            Pair("city", mapOf(Pair("name", "LYON"))),
                            Pair("address", mapOf(Pair("address", "162 AVENUE JEAN JAURÈS")))
                    ),
                    mapOf(
                            Pair("location", mapOf(Pair("zipcode", "69140"))),
                            Pair("city", mapOf(Pair("name", "RILLIEUX-LA-PAPE"))),
                            Pair("address", mapOf(Pair("address", "2871 AVENUE DE L'EUROPE")))
                    ),
                    mapOf(
                            Pair("location", mapOf(Pair("zipcode", "92100"))),
                            Pair("city", mapOf(Pair("name", "BOULOGNE BILLANCOURT"))),
                            Pair("address", mapOf(Pair("address", "8 RUE DE L'EST")))
                    ),
                    mapOf(
                            Pair("location", mapOf(Pair("zipcode", "93240"))),
                            Pair("city", mapOf(Pair("name", "STAINS"))),
                            Pair("address", mapOf(Pair("address", "10-16 RUE DU COLONEL ROL TANGUY\nZAC DU BOIS MOUSSAY")))
                    ),
                    mapOf(
                            Pair("location", mapOf(Pair("zipcode", "94227"))),
                            Pair("city", mapOf(Pair("name", "CHARENTON LE PONT CEDEX"))),
                            Pair("address", mapOf(Pair("address", "2 RUE DUE NOUVEAU BERCY")))
                    )
            )
        }
        assertThat(commitCounter.getCount())
                .overridingErrorMessage("Expected 1 commit")
                .isEqualTo(1)
    }

    @Test
    fun `imports business segments`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute(
                "MATCH (segment:BusinessSegment) " +
                        "RETURN segment {.code, .label} " +
                        "ORDER BY segment.code ASC").use {

            assertThat(it).containsOnlyOnce(
                    row("segment", mapOf(Pair("code", "[AUT]"), Pair("label", "AUTRES"))),
                    row("segment", mapOf(Pair("code", "[DM]"), Pair("label", "DISPOSITIFS MÉDICAUX"))),
                    row("segment", mapOf(Pair("code", "[MED]"), Pair("label", "MÉDICAMENTS"))),
                    row("segment", mapOf(Pair("code", "[PA]"), Pair("label", "PRESTATAIRES ASSOCIÉS")))
            )
        }
        assertThat(commitCounter.getCount())
                .overridingErrorMessage("Expected 1 commit")
                .isEqualTo(1)
    }

    @Test
    fun `imports companies`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute(
                "MATCH (company:Company) " +
                        "RETURN company {.identifier, .name} " +
                        "ORDER BY company.identifier ASC").use {

            assertThat(it).containsOnlyOnce(
                    row("company", mapOf(Pair("identifier", "ARHHJTWT"), Pair("name", "EYETECHCARE"))),
                    row("company", mapOf(Pair("identifier", "FRQXZIGY"), Pair("name", "SANOFI PASTEUR MSD SNC"))),
                    row("company", mapOf(Pair("identifier", "GEJLGPVD"), Pair("name", "NOBEL BIOCARE USA LLC"))),
                    row("company", mapOf(Pair("identifier", "GXIVOHBB"), Pair("name", "ISIS DIABETE"))),
                    row("company", mapOf(Pair("identifier", "MQKQLNIC"), Pair("name", "SIGVARIS"))),
                    row("company", mapOf(Pair("identifier", "OETEUQSP"), Pair("name", "HEALTHCARE COMPLIANCE CONSULTING FRANCE SAS"))),
                    row("company", mapOf(Pair("identifier", "QBSTAWWV"), Pair("name", "IP SANTÉ DOMICILE"))),
                    row("company", mapOf(Pair("identifier", "XSQKIAGK"), Pair("name", "COOK FRANCE SARL"))),
                    row("company", mapOf(Pair("identifier", "ZQKPAZKB"), Pair("name", "CREAFIRST")))
            )
        }
        assertThat(commitCounter.getCount())
                .overridingErrorMessage("Expected 1 commit")
                .isEqualTo(1)
    }

    @Test
    fun `imports address|company|business segment`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute(
                "MATCH (segment:BusinessSegment)<-[:IN_BUSINESS_SEGMENT]-(company:Company)-[:LOCATED_AT_ADDRESS]->(address:Address) " +
                        "RETURN company {.identifier}, segment {.code}, address {.address} " +
                        "ORDER BY company.identifier ASC").use {

            assertThat(it).containsOnlyOnce(
                    mapOf(
                            Pair("company", mapOf(Pair("identifier", "ARHHJTWT"))),
                            Pair("segment", mapOf(Pair("code", "[DM]"))),
                            Pair("address", mapOf(Pair("address", "2871 AVENUE DE L'EUROPE")))
                    ),
                    mapOf(
                            Pair("company", mapOf(Pair("identifier", "FRQXZIGY"))),
                            Pair("segment", mapOf(Pair("code", "[MED]"))),
                            Pair("address", mapOf(Pair("address", "162 AVENUE JEAN JAURÈS")))
                    ),
                    mapOf(
                            Pair("company", mapOf(Pair("identifier", "GEJLGPVD"))),
                            Pair("segment", mapOf(Pair("code", "[DM]"))),
                            Pair("address", mapOf(Pair("address", "800 CORPORATE DRIVE")))
                    ),
                    mapOf(
                            Pair("company", mapOf(Pair("identifier", "GXIVOHBB"))),
                            Pair("segment", mapOf(Pair("code", "[PA]"))),
                            Pair("address", mapOf(Pair("address", "10-16 RUE DU COLONEL ROL TANGUY\nZAC DU BOIS MOUSSAY")))
                    ),
                    mapOf(
                            Pair("company", mapOf(Pair("identifier", "MQKQLNIC"))),
                            Pair("segment", mapOf(Pair("code", "[DM]"))),
                            Pair("address", mapOf(Pair("address", "ZI SUD D'ANDREZIEUX\nRUE B. THIMONNIER")))
                    ),
                    mapOf(
                            Pair("company", mapOf(Pair("identifier", "OETEUQSP"))),
                            Pair("segment", mapOf(Pair("code", "[AUT]"))),
                            Pair("address", mapOf(Pair("address", "47 BOULEVARD CHARLES V")))
                    ),
                    mapOf(
                            Pair("company", mapOf(Pair("identifier", "QBSTAWWV"))),
                            Pair("segment", mapOf(Pair("code", "[PA]"))),
                            Pair("address", mapOf(Pair("address", "16 RUE DE MONTBRILLANT\nBUROPARC RIVE GAUCHE")))
                    ),
                    mapOf(
                            Pair("company", mapOf(Pair("identifier", "XSQKIAGK"))),
                            Pair("segment", mapOf(Pair("code", "[DM]"))),
                            Pair("address", mapOf(Pair("address", "2 RUE DUE NOUVEAU BERCY")))
                    ),
                    mapOf(
                            Pair("company", mapOf(Pair("identifier", "ZQKPAZKB"))),
                            Pair("segment", mapOf(Pair("code", "[PA]"))),
                            Pair("address", mapOf(Pair("address", "8 RUE DE L'EST")))
                    )
            )
        }
        assertThat(commitCounter.getCount())
                .overridingErrorMessage("Expected 1 commit")
                .isEqualTo(1)
    }

    @Test
    fun `batches commits`() {
        newReader("/companies.csv").use {
            subject.import(it, 2)
        }

        assertThat(commitCounter.getCount())
                .overridingErrorMessage("Expected 5 batched commits.")
                .isEqualTo(5)
    }

    @Test
    fun `creates indices for country`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        val countryLabel = Label.label("Country")
        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            Tuple.tuple(countryLabel, listOf("name"), false),
                            Tuple.tuple(countryLabel, listOf("code"), true)
                    )
        }
    }

    @Test
    fun `creates indices for city`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            Tuple.tuple(Label.label("City"), listOf("name"), false)
                    )
        }
    }

    @Test
    fun `creates indices for business segment`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        val businessSegmentLabel = Label.label("BusinessSegment")
        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            Tuple.tuple(businessSegmentLabel, listOf("name"), false),
                            Tuple.tuple(businessSegmentLabel, listOf("code"), true)
                    )
        }
    }

    @Test
    fun `creates indices for companies`() {
        newReader("/companies.csv").use {
            subject.import(it, 100)
        }

        val companyLabel = Label.label("Company")
        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            Tuple.tuple(companyLabel, listOf("name"), false),
                            Tuple.tuple(companyLabel, listOf("identifier"), true)
                    )
        }
    }

    private fun row(name: String, attributes: Map<String, String>) = mapOf(Pair(name, attributes))
}
