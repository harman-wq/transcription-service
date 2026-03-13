const express = require('express');
const { google } = require('googleapis');
const { spawn } = require('child_process');
const FormData = require('form-data');
const axios = require('axios');

const app = express();
app.use(express.json());

// Google Service Account auth
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
  scopes: ['https://www.googleapis.com/auth/drive.readonly'],
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'transcription-service' });
});

// Main transcription endpoint
app.post('/transcribe', async (req, res) => {
  const { drive_file_id } = req.body;

  if (!drive_file_id) {
    return res.status(400).json({ success: false, error: 'Missing drive_file_id' });
  }

  console.log(`[START] Transcribing file: ${drive_file_id}`);
  const startTime = Date.now();

  try {
    // 1. Get file metadata first
    const driveClient = google.drive({ version: 'v3', auth: await auth.getClient() });
    const meta = await driveClient.files.get({
      fileId: drive_file_id,
      fields: 'name,size,mimeType',
    });
    const fileName = meta.data.name;
    const fileSizeMB = (parseInt(meta.data.size || '0') / (1024 * 1024)).toFixed(1);
    console.log(`[META] ${fileName} (${fileSizeMB} MB, ${meta.data.mimeType})`);

    // 2. Stream download from Google Drive
    console.log('[DOWNLOAD] Starting stream from Google Drive...');
    const downloadResponse = await driveClient.files.get(
      { fileId: drive_file_id, alt: 'media' },
      { responseType: 'stream' }
    );

    // 3. Pipe through ffmpeg to extract audio
    // Output: 16kHz mono MP3 at 64kbps (a 5-min video → ~2.4 MB audio)
    console.log('[FFMPEG] Extracting audio...');
    const ffmpeg = spawn('ffmpeg', [
      '-i', 'pipe:0',     // read video from stdin
      '-vn',               // no video
      '-ac', '1',          // mono
      '-ar', '16000',      // 16kHz sample rate (optimal for Whisper)
      '-b:a', '64k',       // 64kbps bitrate
      '-f', 'mp3',         // MP3 format
      'pipe:1',            // output to stdout
    ]);

    // Pipe Drive stream → ffmpeg stdin
    downloadResponse.data.pipe(ffmpeg.stdin);

    // Handle download stream errors
    downloadResponse.data.on('error', (err) => {
      console.error('[DOWNLOAD ERROR]', err.message);
      ffmpeg.stdin.destroy();
    });

    // Collect ffmpeg audio output
    const audioChunks = [];
    ffmpeg.stdout.on('data', (chunk) => audioChunks.push(chunk));

    // Track ffmpeg progress via stderr
    let lastLog = Date.now();
    ffmpeg.stderr.on('data', (data) => {
      const now = Date.now();
      if (now - lastLog > 10000) {
        // Log every 10 seconds
        const line = data.toString().trim().split('\n').pop();
        console.log(`[FFMPEG] ${line.slice(-120)}`);
        lastLog = now;
      }
    });

    // Wait for ffmpeg to finish
    await new Promise((resolve, reject) => {
      ffmpeg.on('close', (code) => {
        if (code === 0) resolve();
        else reject(new Error(`ffmpeg exited with code ${code}`));
      });
      ffmpeg.on('error', reject);
      ffmpeg.stdin.on('error', () => {
        // Ignore EPIPE - happens when ffmpeg closes before download finishes
      });
    });

    const audioBuffer = Buffer.concat(audioChunks);
    const audioSizeMB = (audioBuffer.length / (1024 * 1024)).toFixed(2);
    console.log(`[FFMPEG DONE] Audio extracted: ${audioSizeMB} MB`);

    if (audioBuffer.length === 0) {
      throw new Error('ffmpeg produced no audio output - file may not contain an audio track');
    }

    // 4. Check if audio is under Whisper's 25MB limit
    if (audioBuffer.length > 25 * 1024 * 1024) {
      // Very long video - audio too large even compressed. Re-encode at lower bitrate.
      console.log('[WARNING] Audio > 25MB, re-encoding at 32kbps...');
      const reEncode = spawn('ffmpeg', [
        '-i', 'pipe:0', '-vn', '-ac', '1', '-ar', '16000', '-b:a', '32k', '-f', 'mp3', 'pipe:1',
      ]);

      const reChunks = [];
      reEncode.stdout.on('data', (c) => reChunks.push(c));

      const audioStream = require('stream');
      const readable = new audioStream.Readable();
      readable.push(audioBuffer);
      readable.push(null);
      readable.pipe(reEncode.stdin);

      await new Promise((resolve, reject) => {
        reEncode.on('close', (code) => (code === 0 ? resolve() : reject(new Error('re-encode failed'))));
        reEncode.on('error', reject);
      });

      const reBuffer = Buffer.concat(reChunks);
      console.log(`[RE-ENCODE] New size: ${(reBuffer.length / (1024 * 1024)).toFixed(2)} MB`);
      audioChunks.length = 0;
      audioChunks.push(reBuffer);
    }

    const finalAudio = audioChunks.length === 1 ? audioChunks[0] : Buffer.concat(audioChunks);

    // 5. Send to OpenAI Whisper API
    console.log('[WHISPER] Sending to OpenAI Whisper API...');
    const formData = new FormData();
    formData.append('file', finalAudio, {
      filename: 'audio.mp3',
      contentType: 'audio/mpeg',
    });
    formData.append('model', 'whisper-1');
    formData.append('response_format', 'verbose_json');

    const whisperResponse = await axios.post(
      'https://api.openai.com/v1/audio/transcriptions',
      formData,
      {
        headers: {
          ...formData.getHeaders(),
          Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        },
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 300000, // 5 min timeout for Whisper
      }
    );

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[DONE] Transcription complete in ${elapsed}s. Text length: ${whisperResponse.data.text?.length || 0} chars`);

    res.json({
      success: true,
      text: whisperResponse.data.text || '',
      segments: whisperResponse.data.segments || [],
      duration: whisperResponse.data.duration || 0,
      language: whisperResponse.data.language || 'en',
      file_name: fileName,
      audio_size_mb: parseFloat(audioSizeMB),
      processing_time_seconds: parseFloat(elapsed),
    });
  } catch (error) {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.error(`[ERROR] After ${elapsed}s:`, error.message);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Set server timeout to 15 minutes (large files take time to stream)
const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log(`Transcription service running on port ${PORT}`);
  console.log('Endpoints: GET /health, POST /transcribe');
});
server.timeout = 900000; // 15 minutes
server.keepAliveTimeout = 900000;
