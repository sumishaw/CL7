package com.example.nihongolens

import android.content.Context
import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioManager
import android.media.AudioTrack
import android.speech.tts.TextToSpeech
import android.speech.tts.UtteranceProgressListener
import android.util.Log
import kotlinx.coroutines.*
import java.io.File
import java.util.Locale
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ConcurrentHashMap

/**
 * HindiTtsService — Android TextToSpeech (hi-IN) + Background Music Mixing
 *
 * VOICE QUALITY:
 *   Uses Android's built-in Google TTS (hi-IN locale) which has:
 *   - Real Indian female voice (pitch=1.15f)
 *   - Real Indian male voice (pitch=0.88f)
 *   - No Piper server needed for voice generation
 *   - Near-zero synthesis latency (Google TTS is highly optimized)
 *
 * BACKGROUND MUSIC:
 *   BackgroundMusicRecorder captures USAGE_MEDIA at 44100Hz stereo
 *   Each 2s chunk is POSTed to server /bg?seq=N
 *   At speak() time: snapshot currentBgSeq
 *   Server mixes bg chunk #N under TTS speech at 28% volume
 *   Result: Hindi voice + original background music/sounds in ONE WAV
 *
 * EMOTION ADAPTATION:
 *   GenderAnalyzer sets currentEmotion on this object
 *   Each emotion → pitch + rate adjustment on Android TTS
 *   No Piper params needed — Android TTS handles it natively
 *
 * FIFO + SUBTITLE SYNC:
 *   speak() → fetchQueue → synthesizeToFile → playQueue → PLAY → showTtsText()
 *   Subtitle shown ONLY when audio starts (FIFO token-locked)
 */
object HindiTtsService {

    private const val TAG = "HindiTTS"

    // ── Emotion enum (26 values, used by GenderAnalyzer) ─────────────────────
    enum class Emotion {
        NEUTRAL, HAPPY, SAD, ANGRY, EXCITED, CURIOUS,
        WARM, FEARFUL, SURPRISED, SIGHING,
        SINGING, GASPING, PANTING, MOANING,
        STRAINED, GRAVELLY, RASPY, HUSKY,
        WHISPERY, MURMURED, HUSHED, BREATHY,
        SULTRY, TENDER, VELVETY, DISGUST
    }

    enum class Gender { AUTO, MALE, FEMALE }

    // ── State ─────────────────────────────────────────────────────────────────
    @JvmField @Volatile var enabled           = true
    @JvmField @Volatile var selectedGender    = Gender.AUTO
    @Volatile var ttsSpeedMultiplier          = 1.6f
    @Volatile var detectedGender              = Gender.MALE
    @Volatile var isSpeaking                  = false
    @Volatile var currentEmotion: Emotion     = Emotion.NEUTRAL
    @Volatile private var speakingUntilMs     = 0L

    @JvmField val spokenTokens = ConcurrentHashMap<Int, Boolean>()

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var fetchWorker: Job? = null
    private var playWorker:  Job? = null

    // ── Android TTS ───────────────────────────────────────────────────────────
    private var tts: TextToSpeech? = null
    @Volatile private var ttsReady = false
    private var cacheDir: File? = null
    private var am: AudioManager? = null

    // Pending utterance callbacks: utteranceId → resumption function
    private val pendingUtterances = ConcurrentHashMap<String, () -> Unit>()

    // ── FIFO queues ───────────────────────────────────────────────────────────
    data class FetchItem(
        val text: String,
        val gender: String,
        val pitch: Float,
        val rate: Float,
        val srcText: String = "",
        val emotion: Emotion = Emotion.NEUTRAL,
        val bgSeq: Int = 0,
        val enqMs: Long = System.currentTimeMillis()
    )
    data class PlayItem(val text: String, val wavFile: File, val durMs: Long)

    private val fetchQueue = LinkedBlockingQueue<FetchItem>()
    private val playQueue  = LinkedBlockingQueue<PlayItem>()

    // ── Init ──────────────────────────────────────────────────────────────────

    fun init(context: Context) {
        cacheDir = context.cacheDir
        am = context.getSystemService(Context.AUDIO_SERVICE) as AudioManager

        // Prevent LC from capturing TTS audio
        if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.Q) {
            am?.allowedCapturePolicy = AudioAttributes.ALLOW_CAPTURE_BY_NONE
        }

        // Initialize Android TTS with hi-IN locale
        tts = TextToSpeech(context) { status ->
            if (status == TextToSpeech.SUCCESS) {
                val result = tts?.setLanguage(Locale("hi", "IN"))
                if (result == TextToSpeech.LANG_MISSING_DATA ||
                    result == TextToSpeech.LANG_NOT_SUPPORTED) {
                    // Fallback to generic Hindi
                    tts?.setLanguage(Locale("hi"))
                    CaptionLogger.log(TAG, "TTS: hi-IN not found, using generic hi")
                } else {
                    CaptionLogger.log(TAG, "TTS: hi-IN ready")
                }

                // Set audio attributes to USAGE_ASSISTANT (excluded from LC capture)
                tts?.setAudioAttributes(AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_ASSISTANT)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                    .build())

                // Utterance listener for async synthesis completion
                tts?.setOnUtteranceProgressListener(object : UtteranceProgressListener() {
                    override fun onStart(utteranceId: String?) {}
                    override fun onDone(utteranceId: String?) {
                        utteranceId?.let { id ->
                            pendingUtterances.remove(id)?.invoke()
                        }
                    }
                    override fun onError(utteranceId: String?) {
                        utteranceId?.let { id ->
                            pendingUtterances.remove(id)?.invoke()
                        }
                    }
                })

                ttsReady = true
            } else {
                CaptionLogger.log(TAG, "TTS init failed: $status")
            }
        }

        startFetchWorker()
        startPlayWorker()
    }

    fun setEnabled(on: Boolean) {
        enabled = on
        if (!on) { fetchQueue.clear(); playQueue.clear(); stopAudio() }
    }
    fun setGender(g: Gender)         { selectedGender = g }
    fun setSpeedMultiplier(m: Float) { ttsSpeedMultiplier = m.coerceIn(0.5f, 4.0f) }
    fun isSuppressed()               = isSpeaking || System.currentTimeMillis() < speakingUntilMs

    fun stopAndClear() {
        fetchQueue.clear(); playQueue.clear()
        tts?.stop()
        stopAudio()
        isSpeaking = false
        speakingUntilMs = System.currentTimeMillis() + 300L
        spokenTokens.clear()
    }

    fun destroy() {
        fetchWorker?.cancel(); playWorker?.cancel()
        fetchQueue.clear(); playQueue.clear()
        stopAudio()
        tts?.stop(); tts?.shutdown(); tts = null
        scope.cancel()
    }

    // ── Speak ─────────────────────────────────────────────────────────────────

    fun speak(hindi: String, srcText: String = "") {
        if (!enabled || hindi.isBlank() || !ttsReady) return
        val n = hindi.trim().replace(Regex("\\s+"), " ")

        val token = n.hashCode()
        if (spokenTokens.putIfAbsent(token, true) != null) return
        if (spokenTokens.size > 300) spokenTokens.clear()

        val isFemale = when (selectedGender) {
            Gender.FEMALE -> true
            Gender.MALE   -> false
            Gender.AUTO   -> (detectedGender == Gender.FEMALE)
        }
        val genderTag = if (isFemale) "female" else "male"

        // Emotion → pitch/rate adjustments
        val (pitchMult, rateMult) = emotionPitchRate(currentEmotion)
        val basePitch = if (isFemale) 1.15f else 0.88f
        val finalPitch = (basePitch * pitchMult).coerceIn(0.5f, 2.0f)
        val finalRate  = (ttsSpeedMultiplier * rateMult).coerceIn(0.5f, 3.0f)

        // Snapshot background music sequence at enqueue time for timing alignment
        val bgSeq = BackgroundMusicRecorder.currentSeq.get()

        CaptionLogger.log(TAG, "SPEAK emo=$currentEmotion spd=${String.format("%.2f", finalRate)} " +
            "pitch=${String.format("%.2f", finalPitch)} $genderTag bg=$bgSeq '${n.take(50)}'")

        fetchQueue.offer(FetchItem(n, genderTag, finalPitch, finalRate, srcText,
            currentEmotion, bgSeq, System.currentTimeMillis()))
    }

    // ── Fetch worker: Android TTS → WAV file ──────────────────────────────────

    private fun startFetchWorker() {
        fetchWorker = scope.launch {
            while (isActive) {
                val item = fetchQueue.take()
                if (!enabled || !ttsReady) continue

                // Stale guard: 10s
                val ageMs = System.currentTimeMillis() - item.enqMs
                if (ageMs > 10_000L) {
                    CaptionLogger.log(TAG, "TTS-SKIP stale ${ageMs/1000}s '${item.text.take(30)}'")
                    continue
                }
                // Overload guard: max 2 queued
                if (fetchQueue.size > 2) {
                    CaptionLogger.log(TAG, "TTS-SKIP overloaded q=${fetchQueue.size+1}")
                    continue
                }

                val t0 = System.currentTimeMillis()
                val wavFile = synthesizeToFile(item) ?: continue
                val ms = System.currentTimeMillis() - t0

                // Estimate duration from WAV file size
                val fileSize = wavFile.length()
                val sr = 22050L  // Android TTS default sample rate
                val durMs = if (fileSize > 44) ((fileSize - 44) * 1000L) / (sr * 2L) else 2000L

                CaptionLogger.log(TAG, "TTS-WAV ${ms}ms ${durMs}ms '${item.text.take(40)}'")
                playQueue.offer(PlayItem(item.text, wavFile, durMs))
            }
        }
    }

    // ── Android TTS synthesize to file (async with coroutine bridge) ──────────

    private suspend fun synthesizeToFile(item: FetchItem): File? =
        withContext(Dispatchers.IO) {
            val localTts = tts ?: return@withContext null
            val outFile = File(cacheDir, "tts_${UUID.randomUUID()}.wav")

            // Set voice parameters
            localTts.setSpeechRate(item.rate)
            localTts.setPitch(item.pitch)

            // Bridge Android TTS callback to coroutine
            val id = UUID.randomUUID().toString()
            val deferred = CompletableDeferred<Unit>()

            pendingUtterances[id] = { deferred.complete(Unit) }

            try {
                val result = localTts.synthesizeToFile(
                    item.text,
                    null,
                    outFile,
                    id
                )

                if (result != TextToSpeech.SUCCESS) {
                    pendingUtterances.remove(id)
                    CaptionLogger.log(TAG, "TTS synthesizeToFile failed result=$result")
                    return@withContext null
                }

                // Wait for synthesis to complete (max 8s)
                withTimeoutOrNull(8_000L) { deferred.await() }
                    ?: run {
                        pendingUtterances.remove(id)
                        CaptionLogger.log(TAG, "TTS-TIMEOUT 8s")
                        return@withContext null
                    }

                if (!outFile.exists() || outFile.length() < 100) {
                    CaptionLogger.log(TAG, "TTS output file empty")
                    return@withContext null
                }

                outFile
            } catch (e: Exception) {
                pendingUtterances.remove(id)
                CaptionLogger.log(TAG, "TTS-EXC ${e.message}")
                null
            }
        }

    // ── Play worker: WAV → AudioTrack ─────────────────────────────────────────

    private fun startPlayWorker() {
        playWorker = scope.launch {
            while (isActive) {
                val item = playQueue.take()
                if (!enabled) { item.wavFile.delete(); continue }
                try {
                    isSpeaking = true
                    CaptionLogger.log(TAG, "PLAY ${item.durMs}ms '${item.text.take(40)}'")
                    withContext(Dispatchers.Main) {
                        OverlayService.showTtsText(item.text)
                    }
                    playWavFile(item.wavFile)
                    withContext(Dispatchers.Main) {
                        OverlayService.clearTtsText()
                    }
                    speakingUntilMs = System.currentTimeMillis() + 200L
                } catch (e: Exception) {
                    CaptionLogger.log(TAG, "PLAY-ERR ${e.message}")
                } finally {
                    isSpeaking = false
                    try { item.wavFile.delete() } catch (_: Exception) {}
                }
            }
        }
    }

    // ── AudioTrack playback ───────────────────────────────────────────────────

    private var audioTrack: AudioTrack? = null

    private fun stopAudio() {
        try { audioTrack?.stop() }  catch (_: Exception) {}
        try { audioTrack?.release() } catch (_: Exception) {}
        audioTrack = null
    }

    private suspend fun playWavFile(file: File) = withContext(Dispatchers.IO) {
        try {
            val bytes = file.readBytes()
            if (bytes.size <= 44) return@withContext

            val sr   = readInt(bytes, 24).coerceAtLeast(8_000)
            val nch  = readShort(bytes, 22)
            val bit  = readShort(bytes, 34)
            val pcm  = bytes.copyOfRange(44, bytes.size)
            val fmt  = if (bit == 16) AudioFormat.ENCODING_PCM_16BIT else AudioFormat.ENCODING_PCM_8BIT
            val chan = if (nch == 1) AudioFormat.CHANNEL_OUT_MONO else AudioFormat.CHANNEL_OUT_STEREO
            val dur  = ((pcm.size.toLong() * 1000L) / (sr.toLong() * nch * (bit / 8))).coerceAtLeast(100L)

            val minBuf = AudioTrack.getMinBufferSize(sr, chan, fmt).coerceAtLeast(pcm.size)
            val track = AudioTrack.Builder()
                .setAudioAttributes(AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_ASSISTANT)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                    .build())
                .setAudioFormat(AudioFormat.Builder()
                    .setSampleRate(sr).setChannelMask(chan).setEncoding(fmt).build())
                .setBufferSizeInBytes(minBuf)
                .setTransferMode(AudioTrack.MODE_STATIC)
                .build()

            stopAudio()
            audioTrack = track
            track.write(pcm, 0, pcm.size)
            track.setVolume(AudioTrack.getMaxVolume())
            track.play()
            delay(dur + 150L)
            track.stop(); track.release()
            audioTrack = null
        } catch (e: Exception) {
            Log.e(TAG, "playWavFile: ${e.message}")
        }
    }

    // ── Emotion → pitch + rate multipliers ───────────────────────────────────
    // These modify Android TTS pitch and speech rate to match emotional character

    private fun emotionPitchRate(e: Emotion): Pair<Float, Float> = when (e) {
        Emotion.NEUTRAL    -> Pair(1.00f, 1.00f)
        Emotion.HAPPY      -> Pair(1.05f, 1.08f)
        Emotion.SAD        -> Pair(0.90f, 0.85f)
        Emotion.ANGRY      -> Pair(0.95f, 1.10f)
        Emotion.EXCITED    -> Pair(1.10f, 1.12f)
        Emotion.CURIOUS    -> Pair(1.03f, 0.97f)
        Emotion.WARM       -> Pair(0.97f, 0.93f)
        Emotion.FEARFUL    -> Pair(1.05f, 1.08f)
        Emotion.SURPRISED  -> Pair(1.08f, 1.05f)
        Emotion.SIGHING    -> Pair(0.88f, 0.82f)
        Emotion.SINGING    -> Pair(1.05f, 0.88f)
        Emotion.GASPING    -> Pair(1.10f, 1.18f)
        Emotion.PANTING    -> Pair(1.08f, 1.20f)
        Emotion.MOANING    -> Pair(0.85f, 0.78f)
        Emotion.STRAINED   -> Pair(0.95f, 0.90f)
        Emotion.GRAVELLY   -> Pair(0.88f, 0.95f)
        Emotion.RASPY      -> Pair(0.90f, 0.95f)
        Emotion.HUSKY      -> Pair(0.92f, 0.90f)
        Emotion.WHISPERY   -> Pair(0.92f, 0.88f)
        Emotion.MURMURED   -> Pair(0.90f, 0.85f)
        Emotion.HUSHED     -> Pair(0.88f, 0.85f)
        Emotion.BREATHY    -> Pair(0.93f, 0.90f)
        Emotion.SULTRY     -> Pair(0.88f, 0.83f)
        Emotion.TENDER     -> Pair(0.93f, 0.88f)
        Emotion.VELVETY    -> Pair(0.90f, 0.87f)
        Emotion.DISGUST    -> Pair(0.95f, 1.05f)
    }

    // ── WAV helpers ───────────────────────────────────────────────────────────

    private fun readInt(b: ByteArray, o: Int) =
        ((b[o+3].toInt() and 0xFF) shl 24) or ((b[o+2].toInt() and 0xFF) shl 16) or
        ((b[o+1].toInt() and 0xFF) shl  8) or  (b[o].toInt() and 0xFF)

    private fun readShort(b: ByteArray, o: Int) =
        ((b[o+1].toInt() and 0xFF) shl 8) or (b[o].toInt() and 0xFF)

    // ── Pronoun hint (verb forms only) ────────────────────────────────────────

    fun hasFemininePronouns(src: String): Boolean {
        if (src.isBlank()) return false
        val t = " ${src.lowercase()} "
        return t.contains(" she ") || t.contains(" her ") ||
               t.contains(" herself ") || t.contains(" she's ") ||
               t.contains(" she'd ") || t.contains(" she'll ")
    }

    fun toFeminineHindi(text: String): String {
        var t = text
        t = t.replace("ता हूँ", "ती हूँ").replace("ता हूं", "ती हूं")
        t = t.replace("रहा हूँ", "रही हूँ").replace("रहा हूं", "रही हूं")
        t = t.replace("ता है", "ती है").replace("रहा है", "रही है")
        t = t.replace("ता था", "ती थी").replace("रहा था", "रही थी")
        t = t.replace("गया", "गई").replace("गए", "गईं")
        t = t.replace("आया", "आई").replace("आए", "आईं")
        t = t.replace("किया", "की").replace("लिया", "ली")
        return t
    }
}
