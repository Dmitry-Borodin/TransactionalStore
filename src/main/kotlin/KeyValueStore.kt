package org.example

import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

sealed class Command {
    data class Set(val key: String, val value: String) : Command()
    data class Get(val key: String) : Command()
    data class Delete(val key: String) : Command()
    data class Count(val value: String) : Command()
    data object Begin : Command()
    data object Commit : Command()
    data object Rollback : Command()
    data object Exit : Command()

     companion object {
        fun possibleCommands() = listOf(
            "SET <key> <value>",
            "GET <key>",
            "DELETE <key>",
            "COUNT <value>",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "EXIT"
        )
    }
}

class KeyValueStore {

    private val rwLock = ReentrantReadWriteLock()

    // Thread-safe list of maps representing transactions
    private val dataStack = mutableListOf<MutableMap<String, String>>()

    init {
        dataStack.add(mutableMapOf())
    }

    fun perform(command: Command): Any? {
        return when (command) {
            is Command.Get, is Command.Count -> rwLock.read { executeRead(command) }
            else -> rwLock.write { executeWrite(command) }
        }
    }

    private fun executeRead(command: Command): Any? {
        return when (command) {
            is Command.Get -> get(command.key)
            is Command.Count -> count(command.value)
            else -> throw IllegalArgumentException("Invalid read command")
        }
    }

    private fun executeWrite(command: Command): Any? {
        return when (command) {
            is Command.Set -> set(command.key, command.value)
            is Command.Delete -> delete(command.key)
            Command.Begin -> begin()
            Command.Commit -> commit()
            Command.Rollback -> rollback()
            Command.Exit -> null
            else -> throw IllegalArgumentException("Invalid write command")
        }
    }

    private fun set(key: String, value: String) {
        currentData()[key] = value
    }

    private fun get(key: String): String? {
        return currentData()[key]
    }

    private fun delete(key: String) {
        currentData().remove(key)
    }

    private fun count(value: String): Int {
        return currentData().values.count { it == value }
    }

    private fun begin() {
        dataStack.add(currentData().toMutableMap())
    }

    private fun commit(): Boolean {
        return if (dataStack.size > 1) {
            dataStack[dataStack.lastIndex - 1] = dataStack.last()
            dataStack.removeAt(dataStack.lastIndex)
            true
        } else {
            false
        }
    }

    private fun rollback(): Boolean {
        return if (dataStack.size > 1) {
            dataStack.removeAt(dataStack.lastIndex)
            true
        } else {
            false
        }
    }

    private fun currentData(): MutableMap<String, String> = dataStack.last()
}
