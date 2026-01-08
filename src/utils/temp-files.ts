import { file as tmpFile, dir as tmpDir, DirectoryResult, FileResult } from 'tmp-promise';
import { promises as fs } from 'fs';
import path from 'path';
import { logger } from './logger.js';

export class TempFileManager {
  private files: string[] = [];
  private dirs: string[] = [];
  private baseDir: string;

  constructor(baseDir: string = '/tmp/supabase-sync') {
    this.baseDir = baseDir;
  }

  async init(): Promise<void> {
    await fs.mkdir(this.baseDir, { recursive: true, mode: 0o700 });
    this.dirs.push(this.baseDir);
  }

  async createFile(prefix: string, extension: string = '.sql'): Promise<string> {
    const filePath = path.join(
      this.baseDir,
      `${prefix}-${Date.now()}${extension}`
    );
    await fs.writeFile(filePath, '', { mode: 0o600 });
    this.files.push(filePath);
    return filePath;
  }

  async createDir(prefix: string): Promise<string> {
    const dirPath = path.join(this.baseDir, `${prefix}-${Date.now()}`);
    await fs.mkdir(dirPath, { recursive: true, mode: 0o700 });
    this.dirs.push(dirPath);
    return dirPath;
  }

  getBasePath(): string {
    return this.baseDir;
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

    // Delete files first
    for (const file of this.files) {
      try {
        await this.secureDelete(file);
        logger.debug(`Deleted temp file: ${file}`);
      } catch (error) {
        logger.warn(`Failed to delete temp file: ${file}`, { error });
      }
    }

    // Delete directories (in reverse order to handle nested)
    for (const dir of [...this.dirs].reverse()) {
      try {
        await fs.rm(dir, { recursive: true, force: true });
        logger.debug(`Deleted temp directory: ${dir}`);
      } catch (error) {
        logger.warn(`Failed to delete temp directory: ${dir}`, { error });
      }
    }

    this.files = [];
    this.dirs = [];
  }

  private async secureDelete(filePath: string): Promise<void> {
    try {
      const stats = await fs.stat(filePath);
      // Overwrite with zeros before deleting (for sensitive data)
      if (stats.size > 0 && stats.size < 100 * 1024 * 1024) { // Only for files < 100MB
        const zeros = Buffer.alloc(Math.min(stats.size, 1024 * 1024));
        const handle = await fs.open(filePath, 'r+');
        let written = 0;
        while (written < stats.size) {
          const toWrite = Math.min(zeros.length, stats.size - written);
          await handle.write(zeros, 0, toWrite, written);
          written += toWrite;
        }
        await handle.close();
      }
      await fs.unlink(filePath);
    } catch {
      // Best effort - just try to delete
      await fs.unlink(filePath).catch(() => {});
    }
  }
}
