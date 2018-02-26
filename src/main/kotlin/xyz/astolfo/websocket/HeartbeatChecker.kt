package xyz.astolfo.websocket

import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import org.slf4j.LoggerFactory

/**
 * Helpful class for managing Discord Websocket Heartbeats and making sure its still alive
 *
 * @param scheduledExecutorService - The scheduler for the beats
 * @param sendBeat                 - Runnable that sends the beat to discord
 * @param onDeath                  - Runnable that sends what to do when the heart died
 */
internal class HeartbeatChecker(private val sendBeat: () -> Unit,
                                private val onDeath: () -> Unit) {

    private val heartbeatThreadContext = newFixedThreadPoolContext(2, "Heartbeat Checker")

    private var beatRate: Long = 0

    private var sentBeat: Long = 0
    private var beatPing: Long = 0
    private var receivedLastBeat: Long = 0

    private var shouldBeat = false

    private var heartbeatTask: Job? = null
    private var heartbeatCheckerTask: Job? = null

    /**
     * Starts the heart and beats it at the given rate
     *
     * @param beatRate - the rate in ms
     */
    fun startHeartbeat(beatRate: Long) {
        if (beatRate < 5000)
            throw IllegalArgumentException("Heck, why would the discord heartbeat interval be less then 5000ms?")
        this.beatRate = beatRate
        shouldBeat = true
        stopBeat()
        beat()
    }

    /**
     * Stops to beats from sending and flags that no beats should be
     * sent un till [xyz.astolfo.websocket.HeartbeatChecker.startHeartbeat] is called
     */
    fun stop() {
        shouldBeat = false
        stopBeat()
    }

    /**
     * Cancels the current beating tasks
     */
    private fun stopBeat() {
        heartbeatTask?.cancel()
        stopChecker()
    }

    private fun stopChecker() {
        heartbeatCheckerTask?.cancel()
    }

    /**
     * Starts and schedules a new heartbeat
     */
    private fun newBeat() {
        if (!shouldBeat) return
        log.debug("Scheduled new heartbeat to happen in {}ms", beatRate)
        heartbeatTask = launch(heartbeatThreadContext) {
            delay(beatRate)
            beat()
        }
    }

    /**
     * Sends a heartbeat to discord
     */
    private fun beat() {
        sentBeat = System.currentTimeMillis()
        log.debug("Heartbeat started at {}ms", sentBeat)
        heartbeatCheckerTask = launch(heartbeatThreadContext) {
            delay(beatRate / 2L)
            onDeath.invoke()
        }
        sendBeat.invoke()
        newBeat()
    }

    /**
     * Call when receiving a Heartbeat back from discord
     */
    fun consumeBeat() {
        stopChecker()
        receivedLastBeat = System.currentTimeMillis()
        beatPing = receivedLastBeat - sentBeat
        log.debug("Received heartbeat at {}ms, the Ping is {}ms", receivedLastBeat, beatPing)
    }

    companion object {
        private val log = LoggerFactory.getLogger(HeartbeatChecker::class.java)
    }
}
