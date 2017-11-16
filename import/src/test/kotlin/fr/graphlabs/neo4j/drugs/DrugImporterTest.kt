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
package fr.graphlabs.neo4j.drugs

import fr.graphlabs.neo4j.CommitCounter
import fr.graphlabs.neo4j.Readers.newReader
import fr.graphlabs.neo4j.StringSimilarityFunction
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.groups.Tuple
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.neo4j.graphdb.Label.label
import org.neo4j.harness.junit.Neo4jRule
import org.slf4j.bridge.SLF4JBridgeHandler
import java.io.StringReader
import java.util.*

class DrugImporterTest {

    @get:Rule
    val graphDb = Neo4jRule().withFunction(StringSimilarityFunction::class.java)

    private lateinit var subject: DrugImporter

    private lateinit var commitCounter: CommitCounter

    @Before
    fun prepare() {
        subject = DrugImporter(graphDb.boltURI().toString())
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
    fun `imports drugs`() {
        graphDb.graphDatabaseService.execute("""
            |CREATE (:Company {identifier: 'QBSTAWWV', name:'BOIRON'})
            |CREATE (:Company {identifier: 'GEJLGPVD', name:'PHARMA DEVELOPPEMENT'})
            |CREATE (:Company {identifier: 'ARHHJTWT', name:'MYLAN SAS'})
        """.trimMargin())

        commitCounter.reset()
        newReader("/drugs.tsv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            |MATCH (drug:Drug)
            |RETURN drug {.cis_code, .name}
            |ORDER BY drug.cis_code""".trimMargin()).use {

            assertThat(it).containsExactly(
                    mapOf(Pair("drug", mapOf(
                            Pair("cis_code", "60538772"),
                            Pair("name", "ABIES CANADENSIS BOIRON, DEGRÉ DE DILUTION COMPRIS ENTRE 2CH ET 30CH OU ENTRE 4DH ET 60DH")))),
                    mapOf(Pair("drug", mapOf(
                            Pair("cis_code", "61266250"),
                            Pair("name", "A 313 200 000 UI POUR CENT, POMMADE")))),
                    mapOf(Pair("drug", mapOf(
                            Pair("cis_code", "62170486"),
                            Pair("name", "ABACAVIR/LAMIVUDINE MYLAN PHARMA 600 MG/300 MG, COMPRIMÉ PELLICULÉ")))),
                    mapOf(Pair("drug", mapOf(
                            Pair("cis_code", "62869109"),
                            Pair("name", "A 313 50 000 U.I., CAPSULE MOLLE"))))
            )
        }
        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports drug|lab links`() {
        graphDb.graphDatabaseService.execute("""
            |CREATE (:Company {identifier: 'QBSTAWWV', name:'BOIRON'})
            |CREATE (:Company {identifier: 'GEJLGPVD', name:'PHARMA DEVELOPPEMENT'})
            |CREATE (:Company {identifier: 'ARHHJTWT', name:'MYLAN SAS'})
        """.trimMargin())

        commitCounter.reset()
        newReader("/drugs.tsv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            |MATCH (drug:Drug)-[:DRUG_HELD_BY]->(lab:Company)
            |WITH drug, lab
            |ORDER BY lab.identifier ASC
            |RETURN drug {.cis_code}, COLLECT(lab.identifier) AS labIds
            |ORDER BY drug.cis_code""".trimMargin()).use {

            assertThat(it).containsExactly(
                    mapOf(
                            Pair("drug", mapOf(Pair("cis_code", "60538772"))),
                            Pair("labIds", listOf("QBSTAWWV"))
                    ),
                    mapOf(
                            Pair("drug", mapOf(Pair("cis_code", "61266250"))),
                            Pair("labIds", listOf("GEJLGPVD"))
                    ),
                    mapOf(
                            Pair("drug", mapOf(Pair("cis_code", "62170486"))),
                            Pair("labIds", listOf("ARHHJTWT"))
                    ),
                    mapOf(
                            Pair("drug", mapOf(Pair("cis_code", "62869109"))),
                            Pair("labIds", listOf("ARHHJTWT", "GEJLGPVD"))
                    )
            )
        }
        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports drug|lab links by fuzzy search`() {
        val etalabExportLab = """Labo. Boiron"""
        val ansmExportLab = """Lab. BOIRON"""

        graphDb.graphDatabaseService.execute("CREATE (:Company {identifier: 'SOMEREF.', name:'$etalabExportLab'})")

        commitCounter.reset()
        StringReader("""
60538772	ABIES CANADENSIS BOIRON, degré de dilution compris entre 2CH et 30CH ou entre 4DH et 60DH	 comprimé et solution(s) et granules et poudre et pommade	cutanée;orale;sublinguale	Autorisation active	Enreg homéo (Proc. Nat.)	Commercialisée	20/09/2013			 $ansmExportLab	Non
        """.trimIndent()).use { 
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            |MATCH (drug:Drug)-[:DRUG_HELD_BY]->(lab:Company)
            |WITH drug, lab
            |ORDER BY lab.identifier ASC
            |RETURN drug {.cis_code}, COLLECT(lab.identifier) AS labIds
            |ORDER BY drug.cis_code""".trimMargin()).use {

            assertThat(it).containsExactly(
                    mapOf(
                            Pair("drug", mapOf(Pair("cis_code", "60538772"))),
                            Pair("labIds", listOf("SOMEREF."))
                    )
            )
        }
        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `imports drug|lab links with 1 lab fuzzily matched and 1 not matched`() {
        val etalabExportLab = """Labo. Boiron"""
        val ansmExportLabFuzzyMatch = """Lab. BOIRON"""
        val ansmExportLabNonMatch = """Bonjour tout le monde"""

        graphDb.graphDatabaseService.execute("CREATE (:Company {identifier: 'SOMEREF.', name:'$etalabExportLab'})")

        commitCounter.reset()
        StringReader("""
60538772	ABIES CANADENSIS BOIRON, degré de dilution compris entre 2CH et 30CH ou entre 4DH et 60DH	 comprimé et solution(s) et granules et poudre et pommade	cutanée;orale;sublinguale	Autorisation active	Enreg homéo (Proc. Nat.)	Commercialisée	20/09/2013			 $ansmExportLabFuzzyMatch;$ansmExportLabNonMatch	Non
        """.trimIndent()).use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.execute("""
            |MATCH (drug:Drug)-[:DRUG_HELD_BY]->(lab:Company)
            |WITH drug, lab
            |ORDER BY lab.name ASC
            |RETURN drug.cis_code AS drugCode, COLLECT(lab.name) AS labNames""".trimMargin()).use {

            assertThat(it).containsExactly(
                    mapOf(
                            Pair("drugCode", "60538772"),
                            Pair("labNames", listOf(ansmExportLabNonMatch.toUpperCase(Locale.FRENCH), etalabExportLab))
                    )
            )
        }
        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `batches commits`() {
        newReader("/drugs.tsv").use {
            subject.import(it, commitPeriod = 2)
        }

        assertThat(commitCounter.getCount())
                .overridingErrorMessage("Expected 2 batched commits.")
                .isEqualTo(2)
    }

    @Test
    fun `creates indices for drugs`() {
        newReader("/drugs.tsv").use {
            subject.import(it, 100)
        }

        val drugLabel = label("Drug")
        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            Tuple.tuple(drugLabel, listOf("name"), false),
                            Tuple.tuple(drugLabel, listOf("cis_code"), true)
                    )
        }
    }

    @Test
    fun `creates indices for ANSM unmatched companies`() {
        newReader("/drugs.tsv").use {
            subject.import(it, 100)
        }

        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            Tuple.tuple(label("Ansm"), listOf("name"), false)
                    )
        }
    }
}
