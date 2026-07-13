import { promises as fs } from 'fs';
import path from 'path';
import { logger } from './logger.js';

export class TempFileManager {
  private files: string[] = [];
  private dirs: string[] = [];
  private runDir: string | null = null;

  constructor(private baseDir: string = '/tmp/supabase-sync') {}

  async init(): Promise<void> {
    await fs.mkdir(this.baseDir, { recursive: true, mode: 0o700 });
    this.runDir = await fs.mkdtemp(path.join(this.baseDir, 'run-'));
    this.dirs.push(this.runDir);
  }

  async createFile(prefix: string, extension: string = '.sql'): Promise<string> {
    const filePath = path.join(
      this.getRunDir(),
      `${prefix}-${Date.now()}${extension}`
    );
    await fs.writeFile(filePath, '', { mode: 0o600 });
    this.files.push(filePath);
    return filePath;
  }

  async createDir(prefix: string): Promise<string> {
    const dirPath = path.join(this.getRunDir(), `${prefix}-${Date.now()}`);
    await fs.mkdir(dirPath, { recursive: true, mode: 0o700 });
    this.dirs.push(dirPath);
    return dirPath;
  }

  getBasePath(): string {
    return this.runDir ?? this.baseDir;
  }

  async writeFile(filePath: string, content: string): Promise<void> {
    await fs.writeFile(filePath, content, { mode: 0o600 });
    if (!this.files.includes(filePath)) {
      this.files.push(filePath);
    }
  }

  async readFile(filePath: string): Promise<string> {
    return fs.readFile(filePath, 'utf-8');
  }

  async cleanup(): Promise<void> {
    logger.info('Cleaning up temporary files...');
    const failedFiles: string[] = [];
    const failedDirs: string[] = [];

    // Delete files first
    for (const file of this.files) {
      try {
        await this.secureDelete(file);
        logger.debug(`Deleted temp file: ${file}`);
      } catch (error) {
        logger.warn(`Failed to delete temp file: ${file}`, { error });
        failedFiles.push(file);
      }
    }

    // Delete directories (in reverse order to handle nested)
    for (const dir of [...this.dirs].reverse()) {
      try {
        await fs.rm(dir, { recursive: true, force: true });
        logger.debug(`Deleted temp directory: ${dir}`);
      } catch (error) {
        logger.warn(`Failed to delete temp directory: ${dir}`, { error });
        failedDirs.push(dir);
      }
    }

    this.files = failedFiles;
    this.dirs = failedDirs;
    if (!this.runDir || !failedDirs.includes(this.runDir)) this.runDir = null;
    if (failedFiles.length > 0 || failedDirs.length > 0) {
      throw new Error(`Failed to clean up ${failedFiles.length + failedDirs.length} temporary path(s)`);
    }
  }

  private getRunDir(): string {
    if (!this.runDir) throw new Error('TempFileManager must be initialized before use');
    return this.runDir;
  }

  private async secureDelete(filePath: string): Promise<void> {
    try {
      const stats = await fs.stat(filePath);
      // Overwrite with zeros before deleting (for sensitive data)
      if (stats.size > 0 && stats.size < 100 * 1024 * 1024) { // Only for files < 100MB
        const zeros = Buffer.alloc(Math.min(stats.size, 1024 * 1024));
        const handle = await fs.open(filePath, 'r+');
        try {
          let written = 0;
          while (written < stats.size) {
            const toWrite = Math.min(zeros.length, stats.size - written);
            await handle.write(zeros, 0, toWrite, written);
            written += toWrite;
          }
        } finally {
          await handle.close();
        }
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
      // Overwriting is best effort; deletion below remains mandatory.
    }

    try {
      await fs.unlink(filePath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }
}
