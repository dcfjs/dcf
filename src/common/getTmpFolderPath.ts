export default function getTmpFolderPath() {
  return process.env.tmpdir || 'tmp';
}
