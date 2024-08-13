package org.example

import java.util.*


fun main() {
    val kvStore = KeyValueStore()
    val scanner = Scanner(System.`in`)

    println("Welcome to the Transactional Key-Value Store")
    while (true) {
        print("> ")
        val input = scanner.nextLine().trim().split(" ")
        val command = try {
            parseCommand(input)
        } catch (e: IllegalArgumentException) {
            println("Error: ${e.message}")
            println("Possible commands are:")
            Command.possibleCommands().forEach { println(it) }
            continue
        }

        if (command is Command.Exit) {
            println("Exiting...")
            break
        }

        val result = kvStore.perform(command)
        if (result != null && result != Unit) {
            println(result)
        }
    }
}

fun parseCommand(input: List<String>): Command {
    return when (input[0].uppercase()) {
        "SET" -> {
            if (input.size == 3) Command.Set(input[1], input[2])
            else throw IllegalArgumentException("Usage: SET <key> <value>")
        }
        "GET" -> {
            if (input.size == 2) Command.Get(input[1])
            else throw IllegalArgumentException("Usage: GET <key>")
        }
        "DELETE" -> {
            if (input.size == 2) Command.Delete(input[1])
            else throw IllegalArgumentException("Usage: DELETE <key>")
        }
        "COUNT" -> {
            if (input.size == 2) Command.Count(input[1])
            else throw IllegalArgumentException("Usage: COUNT <value>")
        }
        "BEGIN" -> Command.Begin
        "COMMIT" -> Command.Commit
        "ROLLBACK" -> Command.Rollback
        "EXIT" -> Command.Exit
        else -> throw IllegalArgumentException("Unknown command: ${input[0]}")
    }
}