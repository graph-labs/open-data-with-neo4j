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
package fr.graphlabs.neo4j.benefits

import fr.graphlabs.neo4j.CommitCounter
import fr.graphlabs.neo4j.Readers.newReader
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.groups.Tuple.tuple
import org.junit.*
import org.neo4j.graphdb.Label.label
import org.neo4j.harness.junit.Neo4jRule
import org.slf4j.bridge.SLF4JBridgeHandler

class BenefitImporterTest {

    @get:Rule
    val graphDb = Neo4jRule()

    private lateinit var subject: BenefitImporter

    private lateinit var commitCounter: CommitCounter

    @Before
    fun prepare() {
        subject = BenefitImporter(graphDb.boltURI().toString())
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
    fun `imports medical specialties`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (ms:MedicalSpecialty)
            RETURN ms {.code, .name}
            ORDER BY ms.code ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                    row("ms", mapOf(Pair("code", "[21]"), Pair("name", "PHARMACIEN"))),
                    row("ms", mapOf(Pair("code", "[60]"), Pair("name", "INFIRMIER")))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports health professionals`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (hp:HealthProfessional)
            RETURN hp {.first_name, .last_name}
            ORDER BY hp.first_name ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                    row("hp", mapOf(Pair("first_name", "CATHERINE"), Pair("last_name", "MARQUES"))),
                    row("hp", mapOf(Pair("first_name", "ISABELLE"), Pair("last_name", "BERCERON")))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports health professional|specialty links`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (hp:HealthProfessional)-[:SPECIALIZES_IN]->(ms:MedicalSpecialty)
            RETURN hp {.first_name}, ms {.code}
            ORDER BY ms.code ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                    mapOf(Pair("hp", mapOf(Pair("first_name", "CATHERINE"))), Pair("ms", mapOf(Pair("code", "[21]")))),
                    mapOf(Pair("hp", mapOf(Pair("first_name", "ISABELLE"))), Pair("ms", mapOf(Pair("code", "[60]"))))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports dates of benefits`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (year:Year)<-[:MONTH_IN_YEAR]-(month:Month)<-[:DAY_IN_MONTH]-(day:Day)
            RETURN year {.year}, month {.month}, day {.day}
            ORDER BY year.year, month.month, day.day ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                    mapOf(Pair("year", mapOf(Pair("year", "2014"))), Pair("month", mapOf(Pair("month", "01"))), Pair("day", mapOf(Pair("day", "30")))),
                    mapOf(Pair("year", mapOf(Pair("year", "2014"))), Pair("month", mapOf(Pair("month", "05"))), Pair("day", mapOf(Pair("day", "31")))),
                    mapOf(Pair("year", mapOf(Pair("year", "2014"))), Pair("month", mapOf(Pair("month", "06"))), Pair("day", mapOf(Pair("day", "02")))),
                    mapOf(Pair("year", mapOf(Pair("year", "2014"))), Pair("month", mapOf(Pair("month", "06"))), Pair("day", mapOf(Pair("day", "03"))))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports benefit types`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (bt:BenefitType)
            RETURN bt {.type}
            ORDER BY bt.type ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                    row("bt", mapOf(Pair("type", "REPAS"))),
                    row("bt", mapOf(Pair("type", "TRANSPORT")))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports benefits`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (b:Benefit)
            RETURN b {.amount}
            ORDER BY b.amount ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                    row("b", mapOf(Pair("amount", "25"))),
                    row("b", mapOf(Pair("amount", "38"))),
                    row("b", mapOf(Pair("amount", "45"))),
                    row("b", mapOf(Pair("amount", "48")))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports benefit|benefit date links`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (b:Benefit)-[:GIVEN_AT_DATE]->(day:Day)-[:DAY_IN_MONTH]->(month:Month)-[:MONTH_IN_YEAR]->(year:Year)
            RETURN b {.amount}, day {.day}, month {.month}, year {.year}
            ORDER BY b.amount ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                    mapOf(Pair("b", mapOf(Pair("amount", "25"))), Pair("day", mapOf(Pair("day", "30"))), Pair("month", mapOf(Pair("month", "01"))), Pair("year", mapOf(Pair("year", "2014")))),
                    mapOf(Pair("b", mapOf(Pair("amount", "38"))), Pair("day", mapOf(Pair("day", "03"))), Pair("month", mapOf(Pair("month", "06"))), Pair("year", mapOf(Pair("year", "2014")))),
                    mapOf(Pair("b", mapOf(Pair("amount", "45"))), Pair("day", mapOf(Pair("day", "31"))), Pair("month", mapOf(Pair("month", "05"))), Pair("year", mapOf(Pair("year", "2014")))),
                    mapOf(Pair("b", mapOf(Pair("amount", "48"))), Pair("day", mapOf(Pair("day", "02"))), Pair("month", mapOf(Pair("month", "06"))), Pair("year", mapOf(Pair("year", "2014"))))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports benefit|benefit nature links`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (b:Benefit)-[:HAS_BENEFIT_TYPE]->(bt:BenefitType)
            RETURN b {.amount}, bt {.type}
            ORDER BY b.amount ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                    mapOf(Pair("b", mapOf(Pair("amount", "25"))), Pair("bt", mapOf(Pair("type", "REPAS")))),
                    mapOf(Pair("b", mapOf(Pair("amount", "38"))), Pair("bt", mapOf(Pair("type", "TRANSPORT")))),
                    mapOf(Pair("b", mapOf(Pair("amount", "45"))), Pair("bt", mapOf(Pair("type", "TRANSPORT")))),
                    mapOf(Pair("b", mapOf(Pair("amount", "48"))), Pair("bt", mapOf(Pair("type", "REPAS"))))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports benefit senders aka labs`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (c:Company)
            RETURN c {.identifier}
            ORDER BY c.identifier ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                   row("c", mapOf(Pair("identifier", "IOINTYMQ"))),
                   row("c", mapOf(Pair("identifier", "WFBBGLCU")))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports benefit sender|benefit links`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (c:Company)-[:HAS_GIVEN_BENEFIT]->(b:Benefit)
            RETURN c {.identifier}, b {.amount}
            ORDER BY c.identifier, b.amount ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                   mapOf(Pair("c", mapOf(Pair("identifier", "IOINTYMQ"))), Pair("b", mapOf(Pair("amount", "25")))),
                   mapOf(Pair("c", mapOf(Pair("identifier", "WFBBGLCU"))), Pair("b", mapOf(Pair("amount", "38")))),
                   mapOf(Pair("c", mapOf(Pair("identifier", "WFBBGLCU"))), Pair("b", mapOf(Pair("amount", "45")))),
                   mapOf(Pair("c", mapOf(Pair("identifier", "WFBBGLCU"))), Pair("b", mapOf(Pair("amount", "48"))))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports benefit receiver aka health professional|benefit links`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            MATCH (hp:HealthProfessional)<-[:HAS_RECEIVED_BENEFIT]-(b:Benefit)
            RETURN hp {.first_name}, b {.amount}
            ORDER BY hp.first_name, b.amount ASC""".trimIndent()).use {

            assertThat(it).containsExactly(
                   mapOf(Pair("hp", mapOf(Pair("first_name", "CATHERINE"))), Pair("b", mapOf(Pair("amount", "38")))),
                   mapOf(Pair("hp", mapOf(Pair("first_name", "CATHERINE"))), Pair("b", mapOf(Pair("amount", "45")))),
                   mapOf(Pair("hp", mapOf(Pair("first_name", "CATHERINE"))), Pair("b", mapOf(Pair("amount", "48")))),
                   mapOf(Pair("hp", mapOf(Pair("first_name", "ISABELLE"))), Pair("b", mapOf(Pair("amount", "25"))))
            )
        }

        assertThat(commitCounter.getCount()).isEqualTo(1)
    }



    @Test
    fun `batches commits`() {
        newReader("/benefits.csv").use {
            subject.import(it, 2)
        }

        assertThat(commitCounter.getCount())
                .overridingErrorMessage("Expected 2 batched commits.")
                .isEqualTo(2)
    }

    @Test
    fun `creates indices for health professionals`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            tuple(label("HealthProfessional"), listOf("first_name", "last_name"), false)
                    )
        }
    }

    @Test
    fun `creates indices for medical specialties`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        val medicalSpecialtyLabel = label("MedicalSpecialty")
        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            tuple(medicalSpecialtyLabel, listOf("code"), true)
                    )
        }
    }

    @Test
    fun `creates indices for years, months and days`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            tuple(label("Year"), listOf("year"), true)
                    )
        }
    }

    @Test
    fun `creates indices for benefit types`() {
        newReader("/benefits.csv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            tuple(label("BenefitType"), listOf("type"), true)
                    )
        }
    }

    private fun row(name: String, attributes: Map<String, String>) = mapOf(Pair(name, attributes))

}
