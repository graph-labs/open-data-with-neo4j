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
package fr.graphlabs.neo4j.health_professionals

import fr.graphlabs.neo4j.execute
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.harness.junit.Neo4jRule
import org.slf4j.bridge.SLF4JBridgeHandler

class HealthProfessionalsRepositoryTest {

    @get:Rule
    val neo4j = Neo4jRule()

    lateinit var subject: HealthProfessionalsRepository

    @Before
    fun prepare() {
        subject = HealthProfessionalsRepository(GraphDatabase.driver(neo4j.boltURI()))
    }

    companion object {
        @JvmStatic
        @BeforeClass
        fun prepareAll() {
            SLF4JBridgeHandler.removeHandlersForRootLogger()
        }
    }

    @Test
    fun `find the top 3 health professionals with the most benefits within a year`() {
        neo4j.execute("""
            CREATE (ms1:MedicalSpecialty {code: '[GE]', name: 'Généraliste'})
            CREATE (ms2:MedicalSpecialty {code: '[SE]', name: 'Secrétaire médical(e)'})
            CREATE (hp1:HealthProfessional {first_name: 'Jane1', last_name: 'Doe'})
            CREATE (hp2:HealthProfessional {first_name: 'Jane2', last_name: 'Doe'})
            CREATE (hp3:HealthProfessional {first_name: 'Jane3', last_name: 'Doe'})
            CREATE (hp4:HealthProfessional {first_name: 'Jane4', last_name: 'Doe'})
            CREATE (hp5:HealthProfessional {first_name: 'Jane5', last_name: 'Doe'})
            CREATE (hp1)-[:SPECIALIZES_IN]->(ms1)
            CREATE (hp2)-[:SPECIALIZES_IN]->(ms2)
            CREATE (hp3)-[:SPECIALIZES_IN]->(ms1)
            CREATE (hp4)-[:SPECIALIZES_IN]->(ms2)
            CREATE (hp5)-[:SPECIALIZES_IN]->(ms1)
            CREATE (:Year {year: '2017'})<-[:MONTH_IN_YEAR]-(:Month {month: '04'})<-[:DAY_IN_MONTH]-(d1:Day {day:'01'})
            CREATE (:Year {year: '2017'})<-[:MONTH_IN_YEAR]-(:Month {month: '05'})<-[:DAY_IN_MONTH]-(d2:Day {day:'04'})
            CREATE (:Year {year: '2017'})<-[:MONTH_IN_YEAR]-(:Month {month: '01'})<-[:DAY_IN_MONTH]-(d3:Day {day:'22'})
            CREATE (:Year {year: '2017'})<-[:MONTH_IN_YEAR]-(:Month {month: '12'})<-[:DAY_IN_MONTH]-(d4:Day {day:'11'})
            CREATE (:Year {year: '2017'})<-[:MONTH_IN_YEAR]-(:Month {month: '10'})<-[:DAY_IN_MONTH]-(d5:Day {day:'24'})
            CREATE (:Year {year: '2018'})<-[:MONTH_IN_YEAR]-(:Month {month: '07'})<-[:DAY_IN_MONTH]-(d6:Day {day:'19'})
            CREATE (:Year {year: '2019'})<-[:MONTH_IN_YEAR]-(:Month {month: '06'})<-[:DAY_IN_MONTH]-(d7:Day {day:'13'})
            CREATE (b1:Benefit {amount:'9999'})
            CREATE (b2:Benefit {amount:'12000'})
            CREATE (b3:Benefit {amount:'77000'})
            CREATE (b4:Benefit {amount:'7000'})
            CREATE (b5:Benefit {amount:'500'})
            CREATE (b6:Benefit {amount:'2000'})
            CREATE (b7:Benefit {amount:'37'})
            CREATE (b8:Benefit {amount:'76000'})
            CREATE (bt1:BenefitType {type:'TRANSPORT'})
            CREATE (bt2:BenefitType {type:'RESTAURANT'})
            CREATE (d1)<-[:GIVEN_AT_DATE]-(b1)-[:HAS_BENEFIT_TYPE]->(bt1)
            CREATE (d2)<-[:GIVEN_AT_DATE]-(b2)-[:HAS_BENEFIT_TYPE]->(bt2)
            CREATE (d3)<-[:GIVEN_AT_DATE]-(b3)-[:HAS_BENEFIT_TYPE]->(bt1)
            CREATE (d4)<-[:GIVEN_AT_DATE]-(b4)-[:HAS_BENEFIT_TYPE]->(bt2)
            CREATE (d5)<-[:GIVEN_AT_DATE]-(b5)-[:HAS_BENEFIT_TYPE]->(bt1)
            CREATE (d6)<-[:GIVEN_AT_DATE]-(b6)-[:HAS_BENEFIT_TYPE]->(bt2)
            CREATE (d7)<-[:GIVEN_AT_DATE]-(b7)-[:HAS_BENEFIT_TYPE]->(bt1)
            CREATE (d1)<-[:GIVEN_AT_DATE]-(b8)-[:HAS_BENEFIT_TYPE]->(bt2)
            CREATE (lab1:Company {identifier:'XYZ', name:'XYZ labs'})
            CREATE (lab2:Company {identifier:'XXX', name:'Censored labs'})
            CREATE (lab1)-[:HAS_GIVEN_BENEFIT]->(b1)-[:HAS_RECEIVED_BENEFIT]->(hp1)
            CREATE (lab2)-[:HAS_GIVEN_BENEFIT]->(b2)-[:HAS_RECEIVED_BENEFIT]->(hp2)
            CREATE (lab1)-[:HAS_GIVEN_BENEFIT]->(b3)-[:HAS_RECEIVED_BENEFIT]->(hp2)
            CREATE (lab2)-[:HAS_GIVEN_BENEFIT]->(b4)-[:HAS_RECEIVED_BENEFIT]->(hp4)
            CREATE (lab1)-[:HAS_GIVEN_BENEFIT]->(b5)-[:HAS_RECEIVED_BENEFIT]->(hp5)
            CREATE (lab2)-[:HAS_GIVEN_BENEFIT]->(b6)-[:HAS_RECEIVED_BENEFIT]->(hp2)
            CREATE (lab1)-[:HAS_GIVEN_BENEFIT]->(b7)-[:HAS_RECEIVED_BENEFIT]->(hp3)
            CREATE (lab2)-[:HAS_GIVEN_BENEFIT]->(b8)-[:HAS_RECEIVED_BENEFIT]->(hp1)
        """.trimIndent())

        val healthProfessionals = subject.findTopThreeByMostBenefitsWithinYear("2017")

        val secretarySpecialty = MedicalSpecialty("[SE]", "Secrétaire médical(e)")
        val doctorSpecialty = MedicalSpecialty("[GE]", "Généraliste")
        assertThat(healthProfessionals)
                .containsExactly(
                        Pair(
                            HealthProfessional("Jane2", "Doe", secretarySpecialty),
                            AggregatedBenefits("89000.0",
                                    listOf("Censored labs", "XYZ labs"),
                                    listOf("RESTAURANT", "TRANSPORT"))
                        ),
                        Pair(
                            HealthProfessional("Jane1", "Doe", doctorSpecialty),
                            AggregatedBenefits("85999.0",
                                    listOf("Censored labs", "XYZ labs"),
                                    listOf("RESTAURANT", "TRANSPORT"))
                        ),
                        Pair(
                            HealthProfessional("Jane4", "Doe", secretarySpecialty),
                            AggregatedBenefits("7000.0",
                                    listOf("Censored labs"),
                                    listOf("RESTAURANT"))
                        )
                )
    }

}
