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
package fr.graphlabs.neo4j.labs

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class LabsApiTest {

    private val repository = mock<LabsRepository>()
    private val subject = LabsApi(repository)

    private val labs = listOf(
            Lab("ER54ER53",
                    "Erratum Labs",
                    BusinessSegment("OOPS", "Operative Oculus Partially Salted"),
                    Address("10, Same Old Same Old Street", "01234", City("London", Country("UK", "Unwanted Kale")))),
            Lab("ZZZZZZZZ",
                    "Sleepy Labs",
                    BusinessSegment("P1LL0W", "Plan 1 Leveraging Lots 0f Walruses"),
                    Address("42, Mattress Avenue", "12345", City("Brrrmingham", Country("FR", "Fragile Rest"))))
    )

    @Test
    fun `returns labs matching the marketed drug package`() {
        whenever(repository.findAllByMarketedDrugPackage("some-package")).thenReturn(labs)

        val result = subject.findLabsByMarketedDrug("some-package")

        assertThat(result).isEqualTo(labs)
    }
}
