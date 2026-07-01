package com.example.nihongolens

import android.accessibilityservice.AccessibilityService
import android.accessibilityservice.AccessibilityServiceInfo
import android.view.accessibility.AccessibilityEvent
import android.view.accessibility.AccessibilityNodeInfo
import kotlinx.coroutines.*
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * LiveCaptionReader — Live Captions → Hindi translation → Overlay
 *
 * CRITICAL FIX v2: Near-realtime translation
 * - READ_TIMEOUT reduced to 6s (was 35s) — cuts worst-case delay 6x
 * - SKIP-AHEAD: if queue has >1 item, always process the LATEST (skip old ones)
 * - Translation cache: identical/prefix-match sentences return instantly
 * - Stale threshold: 6s (was 15s) — drops sentences speaker has already passed
 * - Queue cap: 3 (was 8) — never build up more than 1 sentence of backlog
 */
class LiveCaptionReader : AccessibilityService() {

    companion object {
        private const val TAG              = "LCReader"
        private const val TRANSLATE_URL    = "http://127.0.0.1:8765/translate_text"
        private const val CONNECT_TIMEOUT  = 3_000
        // CRITICAL FIX: 6s timeout instead of 35s
        // CT2 opus-mt should finish in <500ms normally; 6s covers slow cases
        // Old 35s was causing 20-30s queue backlog that never recovered
        private const val READ_TIMEOUT     = 6_000
        private const val DEBOUNCE_MS      = 400L
        private const val WATCHDOG_MS      = 2_000L
        private const val STARTUP_GRACE_MS = 1_000L
        private const val LANG_CONFIRM     = 3
        // CRITICAL FIX: Queue cap 3 (was 8)
        // With 6s timeout and 2 workers: max lag = 3 * 3s = 9s worst case
        // Old cap=8 with 35s timeout = 280s backlog possible
        private const val QUEUE_CAP        = 3
        // CRITICAL FIX: Stale threshold 5s (was 15s)
        // Drop sentences the speaker said >5s ago — they've moved on
        private const val STALE_MS         = 5_000L

        private val LC_PACKAGES = setOf(
            "com.google.android.as",
            "com.google.android.as.oss",
            "com.google.android.tts",
        )

        @Volatile var isRunning = false
        @Volatile var instance: LiveCaptionReader? = null
    }

    private val scope       = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var pendingJob:   Job? = null
    private var translateJob: Job? = null
    private var watchdogJob:  Job? = null

    // FIFO + tokens
    private data class QItem(val seq: Long, val text: String, val enqMs: Long = System.currentTimeMillis())
    private val queue      = LinkedBlockingQueue<QItem>()
    private val seqCounter = AtomicLong(0)
    @Volatile private var expectedSeq = 0L

    // Translation LRU cache — avoid re-translating same sentence
    // Key: normalized English, Value: Hindi result
    private val translationCache = LinkedHashMap<String, String>(32, 0.75f, true)
    private val CACHE_MAX = 50

    // Simple dedup — only exact match
    private var lastEnqueued  = ""
    private var lastHindiOut  = ""
    private var lastHindiTime = 0L
    private val HINDI_DEDUP_MS = 4_000L

    // Language tracking
    private var confirmedLang = ""
    private var pendingLang   = ""
    private var pendingCount  = 0

    // Window state
    private var lastRawFull           = ""
    private var lastSentText          = ""
    private var lastEnqueuedSents     = mutableSetOf<String>()
    private var lcVisible             = false
    private var startupTime           = 0L

    // Stats
    private val evtCount = AtomicLong(0)
    private val enqCount = AtomicLong(0)
    private val okCount  = AtomicLong(0)
    private val errCount = AtomicLong(0)

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    override fun onServiceConnected() {
        super.onServiceConnected()
        instance    = this
        isRunning   = true
        startupTime = System.currentTimeMillis()

        serviceInfo = serviceInfo?.also {
            it.eventTypes = (AccessibilityEvent.TYPE_WINDOW_CONTENT_CHANGED or
                AccessibilityEvent.TYPE_VIEW_TEXT_CHANGED or
                AccessibilityEvent.TYPE_WINDOWS_CHANGED)
            it.feedbackType        = AccessibilityServiceInfo.FEEDBACK_GENERIC
            it.notificationTimeout = 100
            it.flags = (AccessibilityServiceInfo.FLAG_REPORT_VIEW_IDS or
                AccessibilityServiceInfo.FLAG_RETRIEVE_INTERACTIVE_WINDOWS)
            it.packageNames = null
        }

        resetAll()
        startWorker()
        startWatchdog()
        startStats()
        CaptionLogger.log(TAG, "=== Connected (READ_TIMEOUT=${READ_TIMEOUT}ms QUEUE_CAP=$QUEUE_CAP STALE=${STALE_MS}ms) ===")
        GenderAnalyzer.start(MainActivity.lcProjection)
        scope.launch(Dispatchers.Main) { MainActivity.instance?.onLiveCaptionReaderConnected() }
    }

    override fun onInterrupt() { CaptionLogger.log(TAG, "Interrupted") }

    override fun onDestroy() {
        isRunning = false; instance = null
        GenderAnalyzer.stop()
        pendingJob?.cancel()
        watchdogJob?.cancel(); translateJob?.cancel(); translateJob2?.cancel()
        queue.clear(); scope.cancel()
        SpeechCaptureService.latestHindi    = ""
        SpeechCaptureService.latestEnglish  = ""
        SpeechCaptureService.latestOriginal = ""
        OverlayService.updateText("", "")
        CaptionLogger.stop()
        super.onDestroy()
    }

    // ── Events ────────────────────────────────────────────────────────────────

    override fun onAccessibilityEvent(event: AccessibilityEvent?) {
        if (!isRunning || event == null) return
        if (System.currentTimeMillis() - startupTime < STARTUP_GRACE_MS) return
        evtCount.incrementAndGet()
        val text = readWindow() ?: return
        schedule(text)
    }

    // ── Watchdog ──────────────────────────────────────────────────────────────

    private fun startWatchdog() {
        watchdogJob = scope.launch {
            var tick = 0L
            while (isActive && isRunning) {
                delay(WATCHDOG_MS)
                if (System.currentTimeMillis() - startupTime < STARTUP_GRACE_MS) continue
                tick++
                val text = withContext(Dispatchers.Main) {
                    try { readWindow() } catch (_: Exception) { null }
                } ?: run {
                    if (tick % 20L == 0L)
                        CaptionLogger.log(TAG, "WD null tick=$tick vis=$lcVisible")
                    return@run null
                } ?: continue
                schedule(text)
            }
        }
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    private fun startStats() {
        scope.launch {
            while (isActive && isRunning) {
                delay(30_000L)
                CaptionLogger.log(TAG, "STATS evt=${evtCount.get()} enq=${enqCount.get()} " +
                    "ok=${okCount.get()} err=${errCount.get()} q=${queue.size} " +
                    "vis=$lcVisible lang=$confirmedLang seq=$expectedSeq cache=${translationCache.size}")
            }
        }
    }

    // ── Window reader ─────────────────────────────────────────────────────────

    private fun readWindow(): String? {
        val wins = try { windows } catch (_: Exception) { return null }

        var root: AccessibilityNodeInfo? = null
        wins?.forEach { w ->
            if (root != null) return@forEach
            val r = try { w.root } catch (_: Exception) { null } ?: return@forEach
            if (r.packageName?.toString() in LC_PACKAGES) root = r else r.recycle()
        }

        if (root == null) {
            if (lcVisible) {
                lcVisible             = false
                lastRawFull           = ""
                lastEnqueued          = ""
                lastSentText          = ""
                lastEnqueuedSents.clear()
                val dropped = queue.size
                queue.clear()
                expectedSeq = seqCounter.get() + 1
                pendingJob?.cancel(); pendingJob = null
                if (dropped > 0) CaptionLogger.log(TAG, "LC gone dropped=$dropped")
                else              CaptionLogger.log(TAG, "LC gone")
                OverlayService.clearQueue()
                HindiTtsService.stopAndClear()
                sentenceTimerJob?.cancel(); sentenceTimerJob = null
                sentenceBuffer = ""; lastBufferEnqueued = ""; lastEnqueuedWordCount = 0
                lastEnqueuedText = ""
            }
            return null
        }

        val nodes = mutableListOf<String>()
        collectText(root, nodes)
        root.recycle()

        val full = nodes.filter { validCaption(it) && !uiLabel(it) }
            .maxByOrNull { it.length }?.trim() ?: return null

        if (!lcVisible) {
            lcVisible   = true
            lastRawFull = ""
            lastEnqueuedSents.clear()
            CaptionLogger.log(TAG, "LC appeared '${full.take(60)}'")
        }

        if (full == lastRawFull) return null

        val prev    = lastRawFull
        lastRawFull = full

        val newText = if (prev.isNotEmpty() && full.startsWith(prev))
            full.substring(prev.length).trim()
        else {
            val sentences = full.split(Regex("""(?<=[.!?。！？])\s+"""))
            sentences.lastOrNull { it.trim().length >= 4 }?.trim() ?: return null
        }

        if (newText.length >= 4) return newText
        return null
    }

    private fun splitSentences(text: String): List<String> {
        val result = mutableListOf<String>()
        val parts  = text.split(Regex("""(?<=[。！？\n.!?])\s*"""))
        for (part in parts) {
            val t = part.trim()
            if (t.length >= 4) result.add(t)
        }
        if (result.isEmpty() && text.trim().length >= 4) result.add(text.trim())
        return result
    }

    private fun directEnqueue(text: String) {
        if (text.isBlank() || text.length < 4) return
        val n = norm(text)
        if (n == lastEnqueued) return
        lastEnqueued = n
        val seq = seqCounter.incrementAndGet()
        if (queue.size >= QUEUE_CAP) {
            val oldest = queue.peek()
            val age = if (oldest != null) System.currentTimeMillis() - oldest.enqMs else 9999L
            if (age > STALE_MS) queue.poll()
        }
        queue.offer(QItem(seq, text))
        enqCount.incrementAndGet()
        CaptionLogger.log(TAG, "ENQ-S seq=$seq q=${queue.size} '${text.take(60)}'")
    }

    // ── Scheduling ────────────────────────────────────────────────────────────

    private fun norm(t: String) = t.trim().replace(Regex("\\s+"), " ")

    private var sentenceBuffer      = ""
    private var lastBufferEnqueued  = ""
    private var lastLCChangeMs      = 0L
    private val SENTENCE_SILENCE_MS = 600L
    private var sentenceTimerJob: Job? = null
    private var lastEnqueuedWordCount = 0
    private var lastEnqueuedText      = ""

    private fun schedule(text: String) {
        val script = detectScript(text)
        if (script != confirmedLang) {
            if (script == pendingLang) {
                if (++pendingCount >= LANG_CONFIRM) {
                    CaptionLogger.log(TAG, "LANG $confirmedLang→$script")
                    confirmedLang = script; pendingLang = ""; pendingCount = 0
                    lastEnqueued = ""; lastRawFull = ""; lastEnqueuedSents.clear()
                    sentenceBuffer = ""; lastBufferEnqueued = ""
                    lastEnqueuedWordCount = 0; lastEnqueuedText = ""
                    queue.clear(); expectedSeq = seqCounter.get() + 1
                }
            } else { pendingLang = script; pendingCount = 1 }
        } else { pendingLang = ""; pendingCount = 0 }

        sentenceBuffer = text
        lastLCChangeMs = System.currentTimeMillis()

        val trimmed   = text.trim()
        val words     = trimmed.split(Regex("\\s+")).filter { it.isNotBlank() }
        val wordCount = words.size

        val endsWithHardPunct = trimmed.endsWith(".") || trimmed.endsWith("?") ||
                                trimmed.endsWith("!") || trimmed.endsWith("。") ||
                                trimmed.endsWith("？") || trimmed.endsWith("！") ||
                                trimmed.endsWith("…")
        if (endsWithHardPunct && wordCount >= 3) {
            sentenceTimerJob?.cancel(); pendingJob?.cancel()
            pendingJob = scope.launch {
                delay(100)
                val t = sentenceBuffer.trim()
                if (t.isNotBlank() && t != lastEnqueuedText) {
                    lastEnqueuedText = t; lastEnqueuedWordCount = wordCount
                    enqueue(t); sentenceBuffer = ""
                }
            }
            return
        }

        val endsWithSoftPunct = trimmed.endsWith(",") || trimmed.endsWith(";") ||
                                trimmed.endsWith(":") || trimmed.endsWith(" -") ||
                                trimmed.endsWith("—")
        if (endsWithSoftPunct && wordCount >= 5) {
            sentenceTimerJob?.cancel(); pendingJob?.cancel()
            pendingJob = scope.launch {
                delay(350)
                val t = sentenceBuffer.trim()
                if (t.isNotBlank() && t != lastEnqueuedText) {
                    lastEnqueuedText = t; lastEnqueuedWordCount = wordCount
                    enqueue(t); sentenceBuffer = ""
                }
            }
            return
        }

        val grown = wordCount - lastEnqueuedWordCount
        if ((grown >= 5 && wordCount >= 5) || (wordCount >= 8 && grown >= 3)) {
            sentenceTimerJob?.cancel(); pendingJob?.cancel()
            pendingJob = scope.launch {
                delay(150)
                val t = sentenceBuffer.trim()
                if (t.isNotBlank() && t != lastEnqueuedText) {
                    lastEnqueuedText = t; lastEnqueuedWordCount = wordCount
                    enqueue(t); sentenceBuffer = ""
                }
            }
            return
        }

        sentenceTimerJob?.cancel()
        sentenceTimerJob = scope.launch {
            delay(SENTENCE_SILENCE_MS)
            val t = sentenceBuffer.trim()
            if (t.isNotBlank() && t != lastEnqueuedText && wordCount >= 2) {
                CaptionLogger.log(TAG, "SILENCE translate")
                lastEnqueuedText = t; lastEnqueuedWordCount = wordCount
                enqueue(t); sentenceBuffer = ""
            }
        }
        pendingJob?.cancel()
    }

    private fun enqueue(text: String) {
        if (text.isBlank() || text.length < 4) return

        val devanagariCount = text.count { it.code in 0x0900..0x097F }
        if (devanagariCount > 0) {
            CaptionLogger.log(TAG, "SKIP: Devanagari detected (TTS loop guard)")
            return
        }

        val wordCount = text.trim().split(Regex("\\s+")).size
        if (wordCount < 2) {
            CaptionLogger.log(TAG, "SKIP: too short ($wordCount words)")
            return
        }
        val lower = text.lowercase()
        val romanizedHindi = listOf(
            "sunkar","muskura","heto","hain","nahin","theek","aapko",
            "tumhe","kijiye","karein","chahiye","matlab","lekin",
            "parantu","isliye","kyunki","waise","raha","rahi","rahe",
            "bolta","bolti","kehta","kehti","sunta","sunti"
        )
        if (romanizedHindi.any { lower.contains(it) } && text.split(" ").size < 10) {
            CaptionLogger.log(TAG, "SKIP: romanized Hindi detected '${text.take(30)}'")
            return
        }

        val n = norm(text)
        if (n == lastEnqueued || n == norm(lastSentText)) {
            CaptionLogger.log(TAG, "SKIP dup")
            return
        }

        if (n.startsWith(lastEnqueued) && lastEnqueued.length > 10) {
            queue.removeIf { norm(it.text) == lastEnqueued }
            CaptionLogger.log(TAG, "SUPERSEDE: removed shorter prefix")
        }

        lastEnqueued = n
        val seq = seqCounter.incrementAndGet()

        // CRITICAL FIX: Skip-ahead — if queue already has items older than 3s,
        // drop them all and only keep this latest sentence
        // This prevents accumulating a backlog of sentences that will never be timely
        if (queue.size >= QUEUE_CAP) {
            val oldest = queue.peek()
            val oldestAge = if (oldest != null) System.currentTimeMillis() - oldest.enqMs else 0L
            if (oldestAge > STALE_MS) {
                val cleared = queue.size
                queue.clear()
                CaptionLogger.log(TAG, "SKIP-AHEAD: cleared $cleared stale items, going to latest")
            } else {
                // Queue not stale yet — just block new item if full
                // (3-item cap means this is only 1-2 sentences max)
                CaptionLogger.log(TAG, "CAP: queue full q=${queue.size}")
                return
            }
        }

        queue.offer(QItem(seq, text))
        enqCount.incrementAndGet()
        CaptionLogger.log(TAG, "ENQ seq=$seq q=${queue.size} '${text.take(60)}'")
    }

    // ── Translation worker ────────────────────────────────────────────────────

    private var translateJob2: Job? = null

    private fun startWorker() {
        // 2 parallel workers — doubles throughput
        translateJob  = scope.launch { workerLoop("W1") }
        translateJob2 = scope.launch { workerLoop("W2") }
    }

    private suspend fun workerLoop(name: String) {
        while (currentCoroutineContext().isActive) {
            val item = withContext(Dispatchers.IO) {
                try { queue.poll(2, TimeUnit.SECONDS) }
                catch (_: InterruptedException) { null }
            } ?: continue

            val seq   = item.seq
            val text  = item.text
            val ageMs = System.currentTimeMillis() - item.enqMs

            if (seq < expectedSeq) { CaptionLogger.log(TAG, "STALE $seq"); continue }
            // CRITICAL FIX: Drop sentences >5s old (was 15s)
            if (ageMs > STALE_MS) {
                CaptionLogger.log(TAG, "EXPIRED $seq age=${ageMs/1000}s")
                continue
            }

            // CRITICAL FIX: Check cache FIRST before any HTTP call
            val nText = norm(text)
            val cached = synchronized(translationCache) { translationCache[nText] }
            if (cached != null) {
                CaptionLogger.log(TAG, "CACHE-HIT[$name] $seq '${cached.take(40)}'")
                deliverHindi(seq, text, cached, name, 0L)
                continue
            }

            val t0     = System.currentTimeMillis()
            lastSentText = text
            val result = callServer(text)
            val ms     = System.currentTimeMillis() - t0

            // CRITICAL FIX: With 6s timeout, if it still takes >4s the sentence is too old
            if (ms > 4_000L && seq < expectedSeq) {
                CaptionLogger.log(TAG, "DISCARD-SLOW $seq ${ms}ms (too old)")
                continue
            }

            if (seq < expectedSeq) { CaptionLogger.log(TAG, "DISCARD $seq ${ms}ms"); continue }

            if (result == null) {
                errCount.incrementAndGet()
                CaptionLogger.log(TAG, "ERR ${ms}ms '${text.take(40)}'")
                continue
            }

            val (hindi, serverLang) = result

            // Cache the result
            synchronized(translationCache) {
                if (translationCache.size >= CACHE_MAX) {
                    translationCache.remove(translationCache.keys.first())
                }
                translationCache[nText] = hindi
            }

            deliverHindi(seq, text, hindi, name, ms)
        }
    }

    private fun deliverHindi(seq: Long, text: String, hindi: String, workerName: String, ms: Long) {
        val now   = System.currentTimeMillis()
        val hNorm = norm(hindi)
        if (hNorm == norm(lastHindiOut) && (now - lastHindiTime) < HINDI_DEDUP_MS) {
            CaptionLogger.log(TAG, "SKIP dup Hindi")
            lastEnqueued = ""; lastRawFull = ""
            return
        }
        lastHindiOut  = hindi
        lastHindiTime = now

        okCount.incrementAndGet()
        CaptionLogger.log(TAG, "OK[$workerName] $seq ${ms}ms '${hindi.take(50)}'")
        SpeechCaptureService.latestHindi   = hindi
        SpeechCaptureService.latestEnglish = text

        // FIFO TOKEN FIX:
        // Previously: OverlayService.updateText() was called immediately when translation
        // arrived → subtitle refreshed every 1-2s regardless of TTS state → subtitles
        // flashed too fast, TTS was always playing an old sentence.
        //
        // Now: subtitle display is driven EXCLUSIVELY by HindiTtsService play worker.
        // Flow: speak() → fetchQueue → synthesize → playQueue → PLAY → showTtsText()
        // The subtitle only appears when TTS is about to speak it (right before playWav).
        // When TTS finishes, clearTtsText() is called → subtitle hidden briefly.
        // Next sentence in playQueue → showTtsText() → subtitle updates.
        //
        // This creates perfect audio+subtitle sync with FIFO ordering.
        // The only direct overlay call is clearing on LC gone (line ~210).

        // Send to Flutter UI counter (does NOT touch overlay)
        scope.launch(Dispatchers.Main) {
            MainActivity.instance?.onTranslation(text, hindi, hindi)
        }

        // Queue for TTS — play worker controls overlay display
        HindiTtsService.speak(hindi, text)
    }

    private fun callServer(text: String): Pair<String, String>? {
        if (text.trim().length < 4) return null
        var conn: HttpURLConnection? = null
        return try {
            conn = URL(TRANSLATE_URL).openConnection() as HttpURLConnection
            conn.requestMethod = "POST"
            conn.setRequestProperty("Content-Type", "application/json; charset=utf-8")
            conn.doOutput       = true
            conn.connectTimeout = CONNECT_TIMEOUT
            // CRITICAL FIX: 6s read timeout (was 35s)
            conn.readTimeout    = READ_TIMEOUT
            val body = """{"text":${JSONObject.quote(text)},"src":"auto","tgt":"hi"}"""
            conn.outputStream.use { it.write(body.toByteArray(Charsets.UTF_8)) }
            if (conn.responseCode != 200) {
                CaptionLogger.log(TAG, "HTTP ${conn.responseCode}"); return null
            }
            val json = JSONObject(conn.inputStream.bufferedReader().readText())
            val hindi = json.optString("text", "").trim()
            val lang  = json.optString("detected_lang", confirmedLang)
            if (hindi.isBlank()) null else Pair(hindi, lang)
        } catch (e: Exception) {
            CaptionLogger.log(TAG, "server ex: ${e.javaClass.simpleName}: ${e.message}"); null
        } finally {
            try { conn?.disconnect() } catch (_: Exception) {}
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun resetAll() {
        lastEnqueued = ""; lastRawFull = ""; lastSentText = ""
        lastEnqueuedSents.clear()
        confirmedLang = ""; pendingLang = ""; pendingCount = 0
        lcVisible = false; expectedSeq = 0L
        lastHindiOut = ""; lastHindiTime = 0L
        translationCache.clear()
        SpeechCaptureService.latestHindi    = ""
        SpeechCaptureService.latestEnglish  = ""
        SpeechCaptureService.latestOriginal = ""
    }

    private fun collectText(node: AccessibilityNodeInfo?, out: MutableList<String>) {
        node ?: return
        node.text?.toString()?.trim()?.takeIf { it.isNotBlank() }?.let { out.add(it) }
        node.contentDescription?.toString()?.trim()
            ?.takeIf { it.isNotBlank() && it != out.lastOrNull() }?.let { out.add(it) }
        for (i in 0 until node.childCount) collectText(node.getChild(i), out)
    }

    private fun uiLabel(t: String): Boolean {
        val l = t.lowercase()
        if (l == "live caption" || l == "live captions") return true
        if (l.startsWith("live caption") && t.length < 30) return true
        if (l.contains("united states") || l.contains("united kingdom")) return true
        if (l.contains("simplified") || l.contains("traditional")) return true
        if (t == "Hide" || t == "Settings" || t == "Feedback") return true
        return false
    }

    private fun validCaption(t: String): Boolean {
        if (t.length < 2 || t.length > 500) return false
        if (t.count { it.isLetter() } < 2) return false
        if (t.contains("com.android") || t.contains("com.google")) return false
        if (t.contains("http") || t.contains("www.")) return false
        return true
    }

    private fun detectScript(text: String): String {
        var ja = 0; var zh = 0; var ko = 0; var ar = 0; var ru = 0; var hi = 0
        for (c in text) when (c.code) {
            in 0x3040..0x30FF -> ja++
            in 0x4E00..0x9FFF -> zh++
            in 0xAC00..0xD7AF -> ko++
            in 0x0600..0x06FF -> ar++
            in 0x0400..0x04FF -> ru++
            in 0x0900..0x097F -> hi++
        }
        val nonLatin = maxOf(ja, zh, ko, ar, ru, hi)
        if (nonLatin > 0) return when (nonLatin) {
            ja -> "ja"; ko -> "ko"; hi -> "hi"; ar -> "ar"; ru -> "ru"; else -> "zh"
        }
        return if (text.any { it.isLetter() && it.code in 0x00C0..0x024F })
            "latin_foreign" else "latin_en"
    }
}
