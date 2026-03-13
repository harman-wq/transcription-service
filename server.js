const express = require('express');
const { google } = require('googleapis');
const { spawn, execFileSync } = require('child_process');
const FormData = require('form-data');
const axios = require('axios');
const fs = require('fs');
const os = require('os');
const path = require('path');

const app = express();
app.use(express.json());

// Google Service Account auth
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
  scopes: ['https://www.googleapis.com/auth/drive.readonly'],
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'transcription-service', version: 2 });
});

// Main transcription endpoint
app.post('/transcribe', async (req, res) => {
  const { drive_file_id } = req.body;

  if (!drive_file_id) {
    return res.status(400).json({ success: false, error: 'Missing drive_file_id' });
  }

  console.log(`[START] Transcribing file: ${drive_file_id}`);
  const startTime = Date.now();
  const tmpFile = path.join(os.tmpdir(), `audio_${Date.now()}.mp3`);

  try {
    // 1. Get file metadata
    const client = await auth.getClient();
    const driveClient = google.drive({ version: 'v3', auth: client });
    const meta = await driveClient.files.get({
      fileId: drive_file_id,
      fields: 'name,size,mimeType',
    });
    const fileName = meta.data.name;
    const fileSizeMB = (parseInt(meta.data.size || '0') / (1024 * 1024)).toFixed(1);
    console.log(`[META] ${fileName} (${fileSizeMB} MB, ${meta.data.mimeType})`);

    // 2. Get a fresh access token for ffmpeg to use directly
    const tokenResponse = await client.getAccessToken();
    const accessToken = tokenResponse.token;
    console.log('[AUTH] Got access token for ffmpeg');

    // 3. Let ffmpeg download and extract audio directly
    // This way Node.js NEVER buffers the huge video file in memory.
    // ffmpeg handles the HTTP download + audio extraction in native code.
    const driveUrl = `https://www.googleapis.com/drive/v3/files/${drive_file_id}?alt=media`;

    console.log('[FFMPEG] Starting direct download + audio extraction...');
    const ffmpeg = spawn('ffmpeg', [
      '-headers', `Authorization: Bearer ${accessToken}\r\n`,
      '-i', driveUrl,
      '-vn',
      '-ac', '1',
      '-ar', '16000',
      '-b:a', '64k',
      '-f', 'mp3',
      tmpFile,
    ]);

    // Track progress
    let lastLog = Date.now();
    let stderrData = '';
    ffmpeg.stderr.on('data', (data) => {
      stderrData += data.toString();
      const now = Date.now();
      if (now - lastLog > 15000) {
        const lines = stderrData.trim().split('\n');
        const lastLine = lines[lines.length - 1];
        console.log(`[FFMPEG] ${lastLine.slice(-150)}`);
        lastLog = now;
      }
    });

    // Wait for ffmpeg to finish
    const ffmpegCode = await new Promise((resolve) => {
      ffmpeg.on('close', resolve);
      ffmpeg.on('error', (err) => {
        console.error('[FFMPEG SPAWN ERROR]', err.message);
        resolve(1);
      });
    });

    if (ffmpegCode !== 0) {
      const lastLines = stderrData.trim().split('\n').slice(-5).join('\n');
      throw new Error(`ffmpeg exited with code ${ffmpegCode}. Last output: ${lastLines.slice(-300)}`);
    }

    // 4. Read the audio file
    if (!fs.existsSync(tmpFile)) {
      throw new Error('ffmpeg did not produce output file');
    }

    const audioBuffer = fs.readFileSync(tmpFile);
    const audioSizeMB = (audioBuffer.length / (1024 * 1024)).toFixed(2);
    console.log(`[FFMPEG DONE] Audio extracted: ${audioSizeMB} MB`);

    fs.unlinkSync(tmpFile);

    if (audioBuffer.length === 0) {
      throw new Error('ffmpeg produced empty audio - file may not contain an audio track');
    }

    // 5. If audio > 25MB, re-encode at lower bitrate
    let finalAudio = audioBuffer;
    if (audioBuffer.length > 25 * 1024 * 1024) {
      console.log('[WARNING] Audio > 25MB, re-encoding at 32kbps...');
      const tmpInput = path.join(os.tmpdir(), `audio_in_${Date.now()}.mp3`);
      const tmpOutput = path.join(os.tmpdir(), `audio_re_${Date.now()}.mp3`);
      fs.writeFileSync(tmpInput, audioBuffer);
      execFileSync('ffmpeg', ['-i', tmpInput, '-vn', '-ac', '1', '-ar', '16000', '-b:a', '32k', '-f', 'mp3', tmpOutput]);
      finalAudio = fs.readFileSync(tmpOutput);
      fs.unlinkSync(tmpOutput);
      fs.unlinkSync(tmpInput);
      console.log(`[RE-ENCODE] New size: ${(finalAudio.length / (1024 * 1024)).toFixed(2)} MB`);
    }

    // 6. Send to OpenAI Whisper API
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
        timeout: 300000,
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
    try { if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile); } catch (e) {}

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.error(`[ERROR] After ${elapsed}s:`, error.message);
    let detail = error.message;
    if (error.response) {
      detail = `HTTP ${error.response.status} from ${error.config?.url?.split('?')[0] || 'unknown'}: ${JSON.stringify(error.response.data).slice(0, 500)}`;
    } else if (error.errors) {
      detail = `Google API: ${JSON.stringify(error.errors).slice(0, 500)}`;
    }
    console.error(`[ERROR DETAIL]`, detail);
    res.status(500).json({
      success: false,
      error: detail,
    });
  }
});

const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log(`Transcription service v2 running on port ${PORT}`);
  console.log('Endpoints: GET /health, POST /transcribe');
});
server.timeout = 900000;
server.keepAliveTimeout = 900000;
