import org.example.Command
import org.example.KeyValueStore
import org.junit.jupiter.api.BeforeEach

import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test

class KeyValueStoreTest {

    private lateinit var kvStore: KeyValueStore

    @BeforeEach
    fun setUp() {
        kvStore = KeyValueStore()
    }

    @Test
    fun `test basic set and get`() {
        kvStore.perform(Command.Set("foo", "123"))
        assertEquals("123", kvStore.perform(Command.Get("foo")))
    }

    @Test
    fun `test delete`() {
        kvStore.perform(Command.Set("foo", "123"))
        kvStore.perform(Command.Delete("foo"))
        assertNull(kvStore.perform(Command.Get("foo")))
    }

    @Test
    fun `test count`() {
        kvStore.perform(Command.Set("foo", "123"))
        kvStore.perform(Command.Set("bar", "123"))
        assertEquals(2, kvStore.perform(Command.Count("123")))
    }

    @Test
    fun `test transaction rollback`() {
        kvStore.perform(Command.Set("foo", "123"))
        kvStore.perform(Command.Begin)
        kvStore.perform(Command.Set("foo", "456"))
        assertEquals("456", kvStore.perform(Command.Get("foo")))
        kvStore.perform(Command.Rollback)
        assertEquals("123", kvStore.perform(Command.Get("foo")))
    }

    @Test
    fun `test transaction commit`() {
        kvStore.perform(Command.Set("foo", "123"))
        kvStore.perform(Command.Begin)
        kvStore.perform(Command.Set("foo", "456"))
        kvStore.perform(Command.Commit)
        assertEquals("456", kvStore.perform(Command.Get("foo")))
    }

    @Test
    fun `test nested transactions`() {
        kvStore.perform(Command.Set("foo", "123"))
        kvStore.perform(Command.Begin)
        kvStore.perform(Command.Set("foo", "456"))
        kvStore.perform(Command.Begin)
        kvStore.perform(Command.Set("foo", "789"))
        assertEquals("789", kvStore.perform(Command.Get("foo")))
        kvStore.perform(Command.Rollback)
        assertEquals("456", kvStore.perform(Command.Get("foo")))
        kvStore.perform(Command.Rollback)
        assertEquals("123", kvStore.perform(Command.Get("foo")))
    }

    @Test
    fun `test rollback without transaction`() {
        assertFalse(kvStore.perform(Command.Rollback) as Boolean)
    }

    @Test
    fun `test commit without transaction`() {
        assertFalse(kvStore.perform(Command.Commit) as Boolean)
    }

    @Test
    fun `test simultaneous read operations`() {
        kvStore.perform(Command.Set("foo", "123"))
        kvStore.perform(Command.Set("bar", "456"))

        val executor = Executors.newFixedThreadPool(2)
        val futures = (1..10).map {
            executor.submit<String> {
                kvStore.perform(Command.Get("foo")) as String + kvStore.perform(Command.Get("bar")) as String
            }
        }
        executor.shutdown()
        executor.awaitTermination(5, TimeUnit.SECONDS)

        futures.forEach { assertEquals("123456", it.get()) }
    }

    @Test
    fun `test write operation with read access`() {
        kvStore.perform(Command.Set("foo", "123"))

        val executor = Executors.newFixedThreadPool(2)
        val readFuture = executor.submit {
            assertEquals("123", kvStore.perform(Command.Get("foo")))
        }

        val writeFuture = executor.submit {
            kvStore.perform(Command.Set("foo", "456"))
        }

        executor.shutdown()
        executor.awaitTermination(5, TimeUnit.SECONDS)

        assertEquals("456", kvStore.perform(Command.Get("foo")))
    }
}