import winston from 'winston';
import chalk from 'chalk';

const { combine, timestamp, printf, colorize } = winston.format;

const customFormat = printf(({ level, message, timestamp, ...metadata }) => {
  let msg = `${timestamp} [${level}]: ${message}`;
  if (Object.keys(metadata).length > 0) {
    msg += ` ${JSON.stringify(metadata)}`;
  }
  return msg;
});

export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: combine(
    timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    customFormat
  ),
  transports: [
    new winston.transports.Console({
      format: combine(
        colorize(),
        timestamp({ format: 'HH:mm:ss' }),
        customFormat
      ),
    }),
  ],
});

export function setLogLevel(level: string): void {
  logger.level = level;
}

// Sanitize sensitive data before logging
export function sanitizeConfig(config: Record<string, unknown>): Record<string, unknown> {
  const sensitiveKeys = ['password', 'serviceRoleKey', 'anonKey', 'dbPassword', 'secret', 'token', 'secretKey', 'publishableKey'];

  const sanitize = (obj: Record<string, unknown>): Record<string, unknown> => {
    const result: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(obj)) {
      if (sensitiveKeys.some(k => key.toLowerCase().includes(k.toLowerCase()))) {
        result[key] = '***REDACTED***';
      } else if (value && typeof value === 'object' && !Array.isArray(value)) {
        result[key] = sanitize(value as Record<string, unknown>);
      } else {
        result[key] = value;
      }
    }

    return result;
  };

  return sanitize(config);
}

// Pretty print helpers
export const print = {
  success: (msg: string) => console.log(chalk.green('✓'), msg),
  error: (msg: string) => console.log(chalk.red('✗'), msg),
  warning: (msg: string) => console.log(chalk.yellow('!'), msg),
  info: (msg: string) => console.log(chalk.blue('ℹ'), msg),
  step: (msg: string) => console.log(chalk.cyan('→'), msg),
  header: (msg: string) => console.log('\n' + chalk.bold.underline(msg) + '\n'),
};
