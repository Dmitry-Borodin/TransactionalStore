package org.example

class KeyValueStore {
    private val dataStack = mutableListOf<MutableMap<String, String>>()
    private val currentData: MutableMap<String, String>
        get() = dataStack.last()

    init {
        // Initialize with an empty transaction
        dataStack.add(mutableMapOf())
    }

    // Store the value for a key
    fun set(key: String, value: String) {
        currentData[key] = value
    }

    // Return the current value for a key
    fun get(key: String): String? {
        return currentData[key]
    }

    // Remove the entry for a key
    fun delete(key: String) {
        currentData.remove(key)
    }

    // Return the number of keys that have the given value
    fun count(value: String): Int {
        return currentData.values.count { it == value }
    }

    // Start a new transaction
    fun begin() {
        // Push a copy of the current state onto the stack
        dataStack.add(currentData.toMutableMap())
    }

    // Commit the current transaction
    fun commit(): Boolean {
        return if (dataStack.size > 1) {
            // Merge the current transaction into the previous one
            val completedTransaction = dataStack.removeAt(dataStack.size - 1)
            dataStack[dataStack.size - 1].putAll(completedTransaction)
            true
        } else {
            false
        }
    }

    // Revert to the state prior to BEGIN call
    fun rollback(): Boolean {
        return if (dataStack.size > 1) {
            dataStack.removeAt(dataStack.size - 1)
            true
        } else {
            false
        }
    }
}
