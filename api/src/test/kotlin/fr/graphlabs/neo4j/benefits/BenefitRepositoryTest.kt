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

import fr.graphlabs.neo4j.labs.Lab
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.harness.junit.Neo4jRule
import org.slf4j.bridge.SLF4JBridgeHandler

class BenefitRepositoryTest {

    @get:Rule
    val neo4j = Neo4jRule()

    lateinit var subject: BenefitRepository

    @Before
    fun prepare() {
        subject = BenefitRepository(GraphDatabase.driver(neo4j.boltURI()))
    }

    companion object {
        @JvmStatic
        @BeforeClass
        fun prepareAll() {
            SLF4JBridgeHandler.removeHandlersForRootLogger()
        }
    }

    @Test
    fun `finds benefits by health professional`() {
        execute("""
                CREATE (hp:HealthProfessional {first_name: 'Jane', last_name: 'Doe'})
                CREATE (:Year {year: '2017'})<-[:MONTH_IN_YEAR]-(m:Month {month: '04'})
                CREATE (m)<-[:DAY_IN_MONTH]-(d:Day {day:'01'})
                CREATE (b:Benefit {amount:'77000'})
                CREATE (bt:BenefitType {type:'TRANSPORT'})
                CREATE (d)<-[:GIVEN_AT_DATE]-(b)-[:HAS_BENEFIT_TYPE]->(bt)
                CREATE (lab:Company {identifier:'XYZ', name:'XYZ labs'})
                CREATE (lab)-[:HAS_GIVEN_BENEFIT]->(b)-[:HAS_RECEIVED_BENEFIT]->(hp)
                """.trimIndent())

        val benefits = subject.findAllByHealthProfessional("Jane", "Doe")

        assertThat(benefits)
                .containsExactly(
                        Benefit(
                                BenefitType("TRANSPORT"),
                                "77000",
                                BenefitDate("2017", "04", "01"),
                                Lab("XYZ", "XYZ labs"))
                )

    }

    private fun execute(cypher: String) {
        val db = neo4j.graphDatabaseService
        db.beginTx().use {
            db.execute(cypher)
            it.success()
        }
    }
}
