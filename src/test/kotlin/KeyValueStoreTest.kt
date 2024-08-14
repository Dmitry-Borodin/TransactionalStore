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
        assertEquals("value for key foo not found", kvStore.perform(Command.Get("foo")))
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
}