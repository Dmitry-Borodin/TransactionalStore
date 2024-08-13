package org.example

import java.util.*


fun main() {
    val kvStore = KeyValueStore()
    val scanner = Scanner(System.`in`)

    println("Welcome to the Transactional Key-Value Store")
    while (true) {
        print("> ")
        val input = scanner.nextLine().trim().split(" ")
        when (val command = input[0].uppercase()) {
            "SET" -> {
                if (input.size == 3) {
                    kvStore.set(input[1], input[2])
                } else {
                    println("Usage: SET <key> <value>")
                }
            }
            "GET" -> {
                if (input.size == 2) {
                    val value = kvStore.get(input[1])
                    if (value != null) {
                        println(value)
                    } else {
                        println("NULL")
                    }
                } else {
                    println("Usage: GET <key>")
                }
            }
            "DELETE" -> {
                if (input.size == 2) {
                    kvStore.delete(input[1])
                    println("Deleted key: ${input[1]}")
                } else {
                    println("Usage: DELETE <key>")
                }
            }
            "COUNT" -> {
                if (input.size == 2) {
                    println(kvStore.count(input[1]))
                } else {
                    println("Usage: COUNT <value>")
                }
            }
            "BEGIN" -> kvStore.begin()
            "COMMIT" -> {
                if (!kvStore.commit()) {
                    println("No transaction to commit")
                } else {
                    println("Transaction committed")
                }
            }
            "ROLLBACK" -> {
                if (!kvStore.rollback()) {
                    println("No transaction to rollback")
                } else {
                    println("Transaction rolled back")
                }
            }
            "EXIT" -> {
                println("Exiting...")
                break
            }
            else -> println("Unknown command: $command")
        }
    }
}