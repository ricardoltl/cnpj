const fs = require('fs');
const path = require('path');
const axios = require('axios');
const cheerio = require('cheerio');
const yaml = require('js-yaml');
const dataIncomingFoldername = 'data_incoming';
const dataOutgoingFoldername = 'data_outgoing';
const logFoldername = 'logs';
const logFilename = 'cnpj_extractor.log';
const configFoldername = 'config';
const configFilename = 'config.yaml';

const pathScript = path.resolve(__filename);
const pathScriptDir = path.dirname(pathScript);
const pathProject = path.dirname(pathScriptDir);
const pathIncoming = path.join(pathProject, dataIncomingFoldername);
const pathOutgoing = path.join(pathProject, dataOutgoingFoldername);
const pathLogDir = path.join(pathProject, logFoldername);
const pathLog = path.join(pathLogDir, logFilename);
const pathConfigDir = path.join(pathProject, configFoldername);
const pathConfig = path.join(pathConfigDir, configFilename);

fs.mkdirSync(pathLogDir, { recursive: true });
fs.mkdirSync(pathIncoming, { recursive: true });

const config = yaml.load(fs.readFileSync(pathConfig, 'utf8'));
const baseUrl = config.base_url;
const MAX_CONCURRENT = 3; // Limite de conexões simultâneas para evitar bloqueio do servidor
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function logLine(level, message) {
  const line = `${new Date().toISOString()} - ${level} - ${message}`;
  fs.appendFileSync(pathLog, `${line}\n`);
  console.log(message);
}

async function getRemoteFileSize(url) {
  try {
    const response = await axios.head(url);
    const size = Number(response.headers['content-length'] || 0);
    return Number.isFinite(size) ? size : 0;
  } catch (error) {
    logLine('ERROR', `Failed to fetch file size for ${url}: ${error.message}`);
    return null;
  }
}

async function getLatestMonthFolder(url) {
  try {
    const response = await axios.get(url);
    const $ = cheerio.load(response.data);
    const folderPattern = /^\d{4}-\d{2}\/?$/;
    const directories = [];

    $('a[href]').each((_, element) => {
      const href = $(element).attr('href');
      if (href && folderPattern.test(href)) {
        directories.push(href.replace(/\/$/, ''));
      }
    });

    directories.sort().reverse();
    return directories[0] || null;
  } catch (error) {
    logLine('ERROR', `Failed to fetch website directories: ${error.message}`);
    return null;
  }
}

async function getAllFilesInFolder(url) {
  try {
    const response = await axios.get(url);
    const $ = cheerio.load(response.data);
    const files = [];

    $('a[href]').each((_, element) => {
      const href = $(element).attr('href');
      if (href && href.endsWith('.zip')) {
        files.push(href);
      }
    });

    return files;
  } catch (error) {
    logLine('ERROR', `Failed to fetch files from folder: ${error.message}`);
    return [];
  }
}

async function downloadFile(url, outputFolder, index, total) {
  const localFileName = url.split('/').pop();
  const localFilePath = path.join(outputFolder, localFileName);
  const progressPrefix = `[${index}/${total}]`;

  const remoteFileSize = await getRemoteFileSize(url);

  if (fs.existsSync(localFilePath) && remoteFileSize) {
    const localFileSize = fs.statSync(localFilePath).size;
    if (localFileSize === remoteFileSize) {
      logLine('INFO', `${progressPrefix} ${localFileName} - already exists (skipped)`);
      return { path: localFilePath, status: 'skipped' };
    }
    logLine('INFO', `${progressPrefix} ${localFileName} - size mismatch, re-downloading...`);
  }

  try {
    const response = await axios.get(url, { responseType: 'stream' });
    const totalSize = Number(response.headers['content-length'] || 0);
    let downloaded = 0;
    let lastLoggedPercent = 0;

    const writer = fs.createWriteStream(localFilePath);

    response.data.on('data', (chunk) => {
      downloaded += chunk.length;
      if (totalSize > 0) {
        const percent = Math.floor((downloaded / totalSize) * 100);
        if (percent >= lastLoggedPercent + 10 || percent === 100) {
          lastLoggedPercent = percent;
          logLine('INFO', `${progressPrefix} ${localFileName} - downloading ${percent}%`);
        }
      }
    });

    await new Promise((resolve, reject) => {
      response.data.pipe(writer);
      writer.on('finish', resolve);
      writer.on('error', reject);
    });

    logLine('INFO', `${progressPrefix} ${localFileName} - downloaded successfully`);
    return { path: localFilePath, status: 'downloaded' };
  } catch (error) {
    logLine('ERROR', `Failed to download ${localFileName}: ${error.message}`);
    try {
      if (fs.existsSync(localFilePath)) {
        fs.unlinkSync(localFilePath);
      }
    } catch (unlinkError) {
      logLine('ERROR', `Failed to remove partial file ${localFileName}: ${unlinkError.message}`);
    }
    return { path: null, status: 'failed' };
  }
}

async function main() {
  const latestFolder = await getLatestMonthFolder(baseUrl);

  if (!latestFolder) {
    logLine('INFO', 'Could not find the latest month folder. Please check the URL or connection.');
    return;
  }

  logLine('INFO', `Latest month folder: ${latestFolder}`);
  const folderUrl = `${baseUrl}/${latestFolder}/`;

  const filesInFolder = await getAllFilesInFolder(folderUrl);
  if (!filesInFolder.length) {
    logLine('INFO', `No files found in the folder: ${latestFolder}`);
    return;
  }

  logLine('INFO', `Found ${filesInFolder.length} files in folder ${latestFolder}`);
  const listOfUrls = filesInFolder.map((file) => `${folderUrl}${file}`);

  const total = listOfUrls.length;
  const results = [];

  // Download sequencial (um arquivo por vez)
  for (let i = 0; i < listOfUrls.length; i++) {
    const result = await downloadFile(listOfUrls[i], pathIncoming, i + 1, total);
    results.push(result);
  }

  const downloaded = results.filter(r => r.status === 'downloaded').length;
  const skipped = results.filter(r => r.status === 'skipped').length;
  const failed = results.filter(r => r.status === 'failed').length;

  logLine('INFO', '----------------------------------------');
  logLine('INFO', `Finished: ${downloaded} downloaded, ${skipped} skipped, ${failed} failed`);
}

main().catch((error) => {
  logLine('ERROR', `Unexpected error: ${error.message}`);
  process.exitCode = 1;
});
