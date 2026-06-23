package com.example.nihongolens

import kotlin.math.*

/**
 * GenderAnalyzer — FFT-based speaker gender detection from raw PCM audio.
 *
 * Called by SpeechCaptureService with each captured audio chunk (internal media audio).
 * DOES NOT use microphone — uses AudioPlaybackCapture (MediaProjection) audio only.
 * Result shared via companion object, consumed by HindiTtsService.
 *
 * Algorithm:
 *   1. Apply Hann window to 2048-sample frame
 *   2. FFT → magnitude spectrum
 *   3. Find dominant frequency peak in 80-400 Hz (vocal fundamental range)
 *   4. Male:   F0 < 165 Hz  (typical 85-155 Hz)
 *   5. Female: F0 ≥ 165 Hz  (typical 165-300 Hz)
 *   6. Smooth over 8 windows to prevent rapid flipping
 */
object GenderAnalyzer {

    @Volatile var detectedGender: HindiTtsService.Gender = HindiTtsService.Gender.MALE
        private set

    private val history    = ArrayDeque<HindiTtsService.Gender>()
    private val HIST_SIZE  = 8
    private val FFT_SIZE   = 2048
    private val SAMPLE_RATE = 16_000

    // Precompute Hann window
    private val hannWindow = FloatArray(FFT_SIZE) { i ->
        (0.5f * (1f - cos(2f * PI.toFloat() * i / (FFT_SIZE - 1))))
    }

    /**
     * Feed raw 16-bit PCM samples to the analyzer.
     * Call from SpeechCaptureService capture loop with each read buffer.
     * Thread-safe — called from capture thread, result read from TTS thread.
     */
    fun feed(pcm: ShortArray, length: Int) {
        if (length < FFT_SIZE) return

        // Use middle section of the buffer for best signal quality
        val offset = (length - FFT_SIZE) / 2
        val re = FloatArray(FFT_SIZE)
        val im = FloatArray(FFT_SIZE)

        // Apply Hann window
        var energy = 0.0
        for (i in 0 until FFT_SIZE) {
            re[i] = pcm[offset + i] * hannWindow[i] / 32768f
            im[i] = 0f
            energy += re[i].toDouble() * re[i]
        }

        // Skip silence — no meaningful pitch in silence
        if (energy / FFT_SIZE < 0.0001) return

        fft(re, im, FFT_SIZE)

        // Find peak in vocal fundamental range 80-400 Hz
        val binLow  = (80f  * FFT_SIZE / SAMPLE_RATE).toInt().coerceAtLeast(1)
        val binHigh = (400f * FFT_SIZE / SAMPLE_RATE).toInt().coerceAtMost(FFT_SIZE / 2)

        var peakBin = binLow
        var peakMag = 0f
        for (b in binLow..binHigh) {
            val mag = sqrt(re[b] * re[b] + im[b] * im[b])
            if (mag > peakMag) { peakMag = mag; peakBin = b }
        }

        // Require minimum magnitude — skip noise
        if (peakMag < 0.001f) return

        val f0 = peakBin.toFloat() * SAMPLE_RATE / FFT_SIZE
        val g  = if (f0 >= 165f) HindiTtsService.Gender.FEMALE else HindiTtsService.Gender.MALE

        // Smooth
        synchronized(history) {
            history.addLast(g)
            if (history.size > HIST_SIZE) history.removeFirst()
            val fCount = history.count { it == HindiTtsService.Gender.FEMALE }
            val result = if (fCount > history.size / 2) HindiTtsService.Gender.FEMALE
                         else HindiTtsService.Gender.MALE
            if (result != detectedGender) {
                detectedGender = result
                android.util.Log.d("GenderAnalyzer",
                    "Gender → $result (F0=${f0.toInt()}Hz mag=${"%.4f".format(peakMag)} " +
                    "f=$fCount/${history.size})")
                // Update HindiTtsService if it's in AUTO mode
                if (HindiTtsService.selectedGender == HindiTtsService.Gender.AUTO) {
                    HindiTtsService.detectedGender = result
                }
            }
        }
    }

    private fun fft(re: FloatArray, im: FloatArray, n: Int) {
        var j = 0
        for (i in 1 until n) {
            var bit = n shr 1
            while (j and bit != 0) { j = j xor bit; bit = bit shr 1 }
            j = j xor bit
            if (i < j) {
                re[i] = re[j].also { re[j] = re[i] }
                im[i] = im[j].also { im[j] = im[i] }
            }
        }
        var len = 2
        while (len <= n) {
            val ang = -2f * PI.toFloat() / len
            val wRe = cos(ang); val wIm = sin(ang)
            var i = 0
            while (i < n) {
                var uRe = 1f; var uIm = 0f
                for (k in 0 until len / 2) {
                    val tRe = uRe * re[i+k+len/2] - uIm * im[i+k+len/2]
                    val tIm = uRe * im[i+k+len/2] + uIm * re[i+k+len/2]
                    re[i+k+len/2] = re[i+k] - tRe
                    im[i+k+len/2] = im[i+k] - tIm
                    re[i+k] += tRe
                    im[i+k] += tIm
                    val nRe = uRe * wRe - uIm * wIm
                    uIm = uRe * wIm + uIm * wRe; uRe = nRe
                }
                i += len
            }
            len = len shl 1
        }
    }
}
