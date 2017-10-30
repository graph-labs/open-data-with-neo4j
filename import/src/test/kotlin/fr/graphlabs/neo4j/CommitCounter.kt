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
package fr.graphlabs.neo4j

import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventHandler
import java.util.concurrent.atomic.AtomicInteger

class CommitCounter : TransactionEventHandler<Any?> {

    private val count = AtomicInteger(0)

    override fun afterRollback(p0: TransactionData?, p1: Any?) {
    }

    override fun beforeCommit(p0: TransactionData?): Any? {
        return null
    }

    override fun afterCommit(p0: TransactionData?, p1: Any?) {
        count.incrementAndGet()
    }

    fun getCount(): Int {
        return count.get()
    }

    fun reset() {
        count.set(0)
    }
}
